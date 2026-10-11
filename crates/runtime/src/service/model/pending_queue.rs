//! Pending invocation lanes and memory-owner scheduling.

use super::*;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::service) enum PendingQueueLane {
    Targeted,
    Memory,
    General,
}

impl PendingQueueLane {
    pub(in crate::service) const ALL: [Self; 3] = [Self::Targeted, Self::Memory, Self::General];
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::service) struct PendingQueueKey {
    lane: PendingQueueLane,
    sequence: u64,
    pub(super) memory_shard_index: Option<usize>,
    memory_owner_key: Option<String>,
}

pub(in crate::service) struct MemoryOwnerQueue {
    pending: BTreeMap<u64, PendingInvoke>,
}

#[derive(Default)]
pub(in crate::service) struct MemoryShardQueue {
    owners: BTreeMap<String, MemoryOwnerQueue>,
    ready: VecDeque<String>,
    ready_membership: HashSet<String>,
    blocked: HashSet<String>,
    queued: usize,
    pub(super) queued_bytes: usize,
}

impl MemoryShardQueue {
    pub(super) fn len(&self) -> usize {
        self.queued
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queued == 0
    }

    fn owner_count(&self) -> usize {
        self.owners.len()
    }

    fn blocked_owner_count(
        &self,
        memory_entity_leases: &HashMap<String, MemoryEntityLease>,
    ) -> usize {
        self.owners
            .keys()
            .filter(|owner_key| {
                self.blocked.contains(*owner_key) || memory_entity_leases.contains_key(*owner_key)
            })
            .count()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &PendingInvoke> {
        self.owners
            .values()
            .flat_map(|owner| owner.pending.values())
    }

    fn insert(
        &mut self,
        owner_key: String,
        sequence: u64,
        pending: PendingInvoke,
    ) -> Option<PendingInvoke> {
        if !self.owners.contains_key(&owner_key) {
            self.owners.insert(
                owner_key.clone(),
                MemoryOwnerQueue {
                    pending: BTreeMap::new(),
                },
            );
            self.ready_owner(owner_key.clone());
        }
        let owner = self
            .owners
            .get_mut(&owner_key)
            .expect("owner queue should exist after insertion");
        let replaced = owner.pending.insert(sequence, pending);
        if let Some(replaced) = replaced.as_ref() {
            self.queued_bytes = self.queued_bytes.saturating_sub(replaced.queued_bytes);
        } else {
            self.queued = self.queued.saturating_add(1);
        }
        if let Some(inserted) = owner.pending.get(&sequence) {
            self.queued_bytes = self.queued_bytes.saturating_add(inserted.queued_bytes);
        }
        replaced
    }

    fn remove(&mut self, owner_key: &str, sequence: u64) -> Option<PendingInvoke> {
        let owner = self.owners.get_mut(owner_key)?;
        let pending = owner.pending.remove(&sequence);
        if let Some(pending) = pending.as_ref() {
            self.queued = self.queued.saturating_sub(1);
            self.queued_bytes = self.queued_bytes.saturating_sub(pending.queued_bytes);
        }
        if owner.pending.is_empty() {
            self.owners.remove(owner_key);
            self.ready_membership.remove(owner_key);
            self.blocked.remove(owner_key);
        }
        pending
    }

    fn find_round_robin_owner_head_map<T>(
        &mut self,
        shard_index: usize,
        map: &mut impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<(PendingQueueKey, u64, T)> {
        let mut inspected = 0usize;
        let ready_len = self.ready.len();
        while inspected < ready_len {
            inspected = inspected.saturating_add(1);
            let owner_key = self.ready.front().cloned()?;
            if !self.ready_membership.contains(&owner_key)
                || self.blocked.contains(&owner_key)
                || !self.owners.contains_key(&owner_key)
            {
                self.ready.pop_front();
                self.ready_membership.remove(&owner_key);
                continue;
            }
            let Some(owner) = self.owners.get(&owner_key) else {
                self.ready.pop_front();
                self.ready_membership.remove(&owner_key);
                continue;
            };
            let Some((sequence, pending)) = owner.pending.first_key_value() else {
                self.ready.pop_front();
                self.ready_membership.remove(&owner_key);
                continue;
            };
            let key = PendingQueueKey {
                lane: PendingQueueLane::Memory,
                sequence: *sequence,
                memory_shard_index: Some(shard_index),
                memory_owner_key: Some(owner_key.clone()),
            };
            let Some(value) = map(key.clone(), pending) else {
                continue;
            };
            return Some((key, *sequence, value));
        }
        None
    }

    fn advance_owner_cursor(&mut self, owner_key: &str) {
        if self
            .ready
            .front()
            .is_some_and(|candidate| candidate == owner_key)
            && let Some(owner_key) = self.ready.pop_front()
        {
            self.ready.push_back(owner_key);
        }
    }

    fn block_owner(&mut self, owner_key: &str) {
        if self.owners.contains_key(owner_key) {
            self.blocked.insert(owner_key.to_string());
            self.ready_membership.remove(owner_key);
        }
    }

    fn ready_owner(&mut self, owner_key: String) -> bool {
        if !self.owners.contains_key(&owner_key) {
            self.blocked.remove(&owner_key);
            self.ready_membership.remove(&owner_key);
            return false;
        }
        self.blocked.remove(&owner_key);
        if self.ready_membership.insert(owner_key.clone()) {
            self.ready.push_back(owner_key);
            return true;
        }
        false
    }
}

pub(in crate::service) struct PendingInvokeQueue {
    next_sequence: u64,
    by_runtime_request_id: HashMap<String, PendingQueueKey>,
    by_target_isolate_id: HashMap<u64, HashSet<PendingQueueKey>>,
    by_memory_owner_key: HashMap<String, HashSet<PendingQueueKey>>,
    by_enqueued_at: BTreeMap<Instant, HashSet<PendingQueueKey>>,
    pub(super) targeted: BTreeMap<u64, PendingInvoke>,
    pub(super) memory_shards: BTreeMap<usize, MemoryShardQueue>,
    memory_next_shard_cursor: Option<usize>,
    pub(super) general: BTreeMap<u64, PendingInvoke>,
    queued: usize,
    pub(super) internal_queued: usize,
    queued_bytes: usize,
}

impl PendingInvokeQueue {
    pub(in crate::service) fn new() -> Self {
        Self {
            next_sequence: 0,
            by_runtime_request_id: HashMap::new(),
            by_target_isolate_id: HashMap::new(),
            by_memory_owner_key: HashMap::new(),
            by_enqueued_at: BTreeMap::new(),
            targeted: BTreeMap::new(),
            memory_shards: BTreeMap::new(),
            memory_next_shard_cursor: None,
            general: BTreeMap::new(),
            queued: 0,
            internal_queued: 0,
            queued_bytes: 0,
        }
    }

    pub(in crate::service) fn is_empty(&self) -> bool {
        self.queued == 0
    }

    pub(in crate::service) fn len(&self) -> usize {
        self.queued
    }

    pub(in crate::service) fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub(in crate::service) fn lane_depths(&self) -> PendingQueueLaneDepths {
        PendingQueueLaneDepths {
            targeted: self.targeted.len(),
            memory: self
                .memory_shards
                .values()
                .map(MemoryShardQueue::len)
                .sum::<usize>(),
            general: self.general.len(),
        }
    }

    pub(in crate::service) fn active_memory_shards(&self) -> usize {
        self.memory_shards.len()
    }

    pub(in crate::service) fn memory_shard_depth_summary(&self) -> (usize, usize) {
        let mut depths = self
            .memory_shards
            .values()
            .map(MemoryShardQueue::len)
            .collect::<Vec<_>>();
        if depths.is_empty() {
            return (0, 0);
        }
        depths.sort_unstable();
        let max = depths.last().copied().unwrap_or(0);
        let median = depths[depths.len() / 2];
        (max, median)
    }

    pub(in crate::service) fn memory_owner_queues(&self) -> usize {
        self.memory_shards
            .values()
            .map(MemoryShardQueue::owner_count)
            .sum::<usize>()
    }

    pub(in crate::service) fn blocked_memory_owner_queues(
        &self,
        memory_entity_leases: &HashMap<String, MemoryEntityLease>,
    ) -> usize {
        self.memory_shards
            .values()
            .map(|shard| shard.blocked_owner_count(memory_entity_leases))
            .sum::<usize>()
    }

    pub(in crate::service) fn mark_memory_owner_blocked(&mut self, key: &PendingQueueKey) {
        let (Some(shard_index), Some(owner_key)) =
            (key.memory_shard_index, key.memory_owner_key.as_deref())
        else {
            return;
        };
        if let Some(memory_shard) = self.memory_shards.get_mut(&shard_index) {
            memory_shard.block_owner(owner_key);
        }
    }

    pub(super) fn mark_memory_owner_ready(&mut self, owner_key: &str) -> bool {
        let Some(keys) = self.by_memory_owner_key.get(owner_key) else {
            return false;
        };
        let Some(shard_index) = keys.iter().find_map(|key| key.memory_shard_index) else {
            return false;
        };
        let Some(memory_shard) = self.memory_shards.get_mut(&shard_index) else {
            return false;
        };
        memory_shard.ready_owner(owner_key.to_string())
    }

    pub(in crate::service) fn oldest_queue_age_ms(&self, now: Instant) -> u64 {
        self.by_enqueued_at
            .keys()
            .next()
            .map(|enqueued_at| duration_millis_u64(now.saturating_duration_since(*enqueued_at)))
            .unwrap_or(0)
    }

    pub(in crate::service) fn memory_shard_debug(
        &self,
        memory_entity_leases: &HashMap<String, MemoryEntityLease>,
        memory_shard_affinity: &HashMap<usize, u64>,
        isolate_indices: &HashMap<u64, usize>,
        limit: usize,
    ) -> Vec<MemoryShardDebug> {
        let mut shards = self
            .memory_shards
            .iter()
            .map(|(shard_index, shard)| {
                let blocked_owners = shard.blocked_owner_count(memory_entity_leases);
                let affinity_isolate_id = memory_shard_affinity.get(shard_index).copied();
                MemoryShardDebug {
                    shard_index: *shard_index,
                    queued: shard.len(),
                    ready_owners: shard.owner_count().saturating_sub(blocked_owners),
                    blocked_owners,
                    affinity_isolate_id,
                    affinity_stale: affinity_isolate_id
                        .map(|isolate_id| !isolate_indices.contains_key(&isolate_id))
                        .unwrap_or(false),
                }
            })
            .collect::<Vec<_>>();
        shards.sort_by(|left, right| {
            right
                .queued
                .cmp(&left.queued)
                .then_with(|| left.shard_index.cmp(&right.shard_index))
        });
        shards.truncate(limit);
        shards
    }

    pub(in crate::service) fn push_back(&mut self, pending: PendingInvoke) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let lane = Self::lane_for(&pending);
        let key = Self::key_for(lane, sequence, &pending);
        self.index_pending(key.clone(), &pending);
        let replaced = self.insert_pending(key, pending);
        debug_assert!(replaced.is_none());
    }

    pub(in crate::service) fn pop_front(&mut self) -> Option<PendingInvoke> {
        let key = self.oldest_key(PendingQueueLane::ALL)?;
        self.remove(key)
    }

    pub(in crate::service) fn remove(&mut self, key: PendingQueueKey) -> Option<PendingInvoke> {
        let pending = self.remove_pending_from_lane(key.clone())?;
        self.unindex_pending(key, &pending);
        Some(pending)
    }

    pub(in crate::service) fn remove_by_runtime_request_id(
        &mut self,
        runtime_request_id: &str,
    ) -> Option<PendingInvoke> {
        let key = self
            .by_runtime_request_id
            .get(runtime_request_id)
            .cloned()?;
        let removed = self.remove(key);
        if removed.is_none() {
            self.by_runtime_request_id.remove(runtime_request_id);
        }
        removed
    }

    pub(in crate::service) fn drain_target_isolate_id(
        &mut self,
        target_isolate_id: u64,
    ) -> Vec<PendingInvoke> {
        let Some(keys) = self.by_target_isolate_id.remove(&target_isolate_id) else {
            return Vec::new();
        };
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort_by_key(|key| key.sequence);
        keys.into_iter()
            .filter_map(|key| {
                let pending = self.remove_pending_from_lane(key.clone())?;
                self.unindex_pending(key, &pending);
                Some(pending)
            })
            .collect()
    }

    pub(in crate::service) fn drain_expired(
        &mut self,
        now: Instant,
        max_queue_wait: Duration,
    ) -> Vec<PendingInvoke> {
        let Some(cutoff) = now.checked_sub(max_queue_wait) else {
            return Vec::new();
        };
        let expired_instants = self
            .by_enqueued_at
            .range(..=cutoff)
            .map(|(enqueued_at, _)| *enqueued_at)
            .collect::<Vec<_>>();
        let mut keys = Vec::new();
        for enqueued_at in expired_instants {
            if let Some(indexed_keys) = self.by_enqueued_at.remove(&enqueued_at) {
                keys.extend(indexed_keys);
            }
        }
        keys.sort_by_key(|key| key.sequence);
        keys.into_iter()
            .filter_map(|key| {
                let pending = self.remove_pending_from_lane(key.clone())?;
                self.unindex_pending(key, &pending);
                Some(pending)
            })
            .collect()
    }

    pub(in crate::service) fn next_expiry_at(&self, max_queue_wait: Duration) -> Option<Instant> {
        self.by_enqueued_at
            .keys()
            .next()
            .map(|enqueued_at| *enqueued_at + max_queue_wait)
    }

    pub(super) fn find_oldest_map<T>(
        &self,
        lanes: impl IntoIterator<Item = PendingQueueLane>,
        mut map: impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<T> {
        let mut selected = None;
        for lane in lanes {
            match lane {
                PendingQueueLane::Targeted | PendingQueueLane::General => {
                    for (sequence, pending) in self.lane(lane) {
                        let key = PendingQueueKey {
                            lane,
                            sequence: *sequence,
                            memory_shard_index: None,
                            memory_owner_key: None,
                        };
                        let Some(value) = map(key, pending) else {
                            continue;
                        };
                        if selected
                            .as_ref()
                            .is_none_or(|(selected_sequence, _)| sequence < selected_sequence)
                        {
                            selected = Some((*sequence, value));
                        }
                    }
                }
                PendingQueueLane::Memory => {
                    for (shard_index, memory_shard) in &self.memory_shards {
                        for (owner_key, owner_queue) in &memory_shard.owners {
                            let Some((sequence, pending)) = owner_queue.pending.first_key_value()
                            else {
                                continue;
                            };
                            let key = PendingQueueKey {
                                lane,
                                sequence: *sequence,
                                memory_shard_index: Some(*shard_index),
                                memory_owner_key: Some(owner_key.clone()),
                            };
                            let Some(value) = map(key, pending) else {
                                continue;
                            };
                            if selected
                                .as_ref()
                                .is_none_or(|(selected_sequence, _)| sequence < selected_sequence)
                            {
                                selected = Some((*sequence, value));
                            }
                        }
                    }
                }
            }
        }
        selected.map(|(_, value)| value)
    }

    pub(in crate::service) fn find_fair_map<T>(
        &mut self,
        lanes: impl IntoIterator<Item = PendingQueueLane>,
        mut map: impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<T> {
        let mut selected = None;
        let mut selected_key = None;
        for lane in lanes {
            let candidate = match lane {
                PendingQueueLane::Memory => self.find_memory_round_robin_head_map(&mut map),
                PendingQueueLane::Targeted | PendingQueueLane::General => {
                    self.find_lane_oldest_map(lane, &mut map)
                }
            };
            let Some((key, sequence, value)) = candidate else {
                continue;
            };
            if selected
                .as_ref()
                .is_none_or(|(selected_sequence, _)| sequence < *selected_sequence)
            {
                selected = Some((sequence, value));
                selected_key = Some(key);
            }
        }
        if matches!(
            selected_key.as_ref(),
            Some(PendingQueueKey {
                lane: PendingQueueLane::Memory,
                memory_shard_index: Some(_),
                ..
            })
        ) {
            let selected_key = selected_key
                .as_ref()
                .expect("selected memory key must exist");
            self.advance_memory_cursor(selected_key);
        }
        selected.map(|(_, value)| value)
    }

    pub(in crate::service) fn iter(&self) -> impl Iterator<Item = &PendingInvoke> {
        self.targeted
            .values()
            .chain(self.memory_shards.values().flat_map(MemoryShardQueue::iter))
            .chain(self.general.values())
    }

    pub(in crate::service) fn has_internal_request(&self) -> bool {
        self.internal_queued > 0
    }

    pub(in crate::service) fn has_external_request(&self) -> bool {
        self.queued > self.internal_queued
    }

    fn oldest_key(
        &self,
        lanes: impl IntoIterator<Item = PendingQueueLane>,
    ) -> Option<PendingQueueKey> {
        self.find_oldest_map(lanes, |key, _| Some(key))
    }

    fn lane_for(pending: &PendingInvoke) -> PendingQueueLane {
        if pending.target_isolate_id.is_some() {
            PendingQueueLane::Targeted
        } else if pending.memory_route.is_some() {
            PendingQueueLane::Memory
        } else {
            PendingQueueLane::General
        }
    }

    fn key_for(lane: PendingQueueLane, sequence: u64, pending: &PendingInvoke) -> PendingQueueKey {
        PendingQueueKey {
            lane,
            sequence,
            memory_shard_index: (lane == PendingQueueLane::Memory)
                .then(|| Self::memory_shard_index_for(pending)),
            memory_owner_key: (lane == PendingQueueLane::Memory)
                .then(|| Self::memory_owner_key_for(pending)),
        }
    }

    fn memory_shard_index_for(pending: &PendingInvoke) -> usize {
        pending
            .memory_route
            .as_ref()
            .and_then(|route| route.shard_index)
            .unwrap_or(0)
    }

    fn memory_owner_key_for(pending: &PendingInvoke) -> String {
        pending
            .memory_route
            .as_ref()
            .map(|route| route.owner_key.clone())
            .unwrap_or_default()
    }

    fn insert_pending(
        &mut self,
        key: PendingQueueKey,
        pending: PendingInvoke,
    ) -> Option<PendingInvoke> {
        let queued_bytes = pending.queued_bytes;
        let replaced = match key.lane {
            PendingQueueLane::Targeted => self.targeted.insert(key.sequence, pending),
            PendingQueueLane::Memory => self
                .memory_shards
                .entry(key.memory_shard_index.unwrap_or(0))
                .or_default()
                .insert(
                    key.memory_owner_key.clone().unwrap_or_default(),
                    key.sequence,
                    pending,
                ),
            PendingQueueLane::General => self.general.insert(key.sequence, pending),
        };
        if let Some(replaced) = replaced.as_ref() {
            self.queued_bytes = self.queued_bytes.saturating_sub(replaced.queued_bytes);
        } else {
            self.queued = self.queued.saturating_add(1);
        }
        self.queued_bytes = self.queued_bytes.saturating_add(queued_bytes);
        replaced
    }

    fn remove_pending_from_lane(&mut self, key: PendingQueueKey) -> Option<PendingInvoke> {
        let pending = match key.lane {
            PendingQueueLane::Targeted => self.targeted.remove(&key.sequence),
            PendingQueueLane::Memory => {
                let shard_index = key.memory_shard_index.unwrap_or(0);
                let (pending, remove_shard) = {
                    let memory_shard = self.memory_shards.get_mut(&shard_index)?;
                    let pending = memory_shard.remove(
                        key.memory_owner_key.as_deref().unwrap_or_default(),
                        key.sequence,
                    );
                    (pending, memory_shard.is_empty())
                };
                if remove_shard {
                    self.memory_shards.remove(&shard_index);
                }
                pending
            }
            PendingQueueLane::General => self.general.remove(&key.sequence),
        };
        if let Some(pending) = pending.as_ref() {
            self.queued = self.queued.saturating_sub(1);
            self.queued_bytes = self.queued_bytes.saturating_sub(pending.queued_bytes);
        }
        pending
    }

    fn index_pending(&mut self, key: PendingQueueKey, pending: &PendingInvoke) {
        if pending.internal_origin {
            self.internal_queued = self.internal_queued.saturating_add(1);
        }
        self.by_runtime_request_id
            .insert(pending.runtime_request_id.clone(), key.clone());
        if let Some(target_isolate_id) = pending.target_isolate_id {
            self.by_target_isolate_id
                .entry(target_isolate_id)
                .or_default()
                .insert(key.clone());
        }
        if let Some(memory_route) = pending.memory_route.as_ref() {
            self.by_memory_owner_key
                .entry(memory_route.owner_key.clone())
                .or_default()
                .insert(key.clone());
        }
        self.by_enqueued_at
            .entry(pending.enqueued_at)
            .or_default()
            .insert(key);
    }

    fn unindex_pending(&mut self, key: PendingQueueKey, pending: &PendingInvoke) {
        if pending.internal_origin {
            self.internal_queued = self.internal_queued.saturating_sub(1);
        }
        self.by_runtime_request_id
            .remove(&pending.runtime_request_id);
        if let Some(target_isolate_id) = pending.target_isolate_id {
            let remove_target_entry =
                if let Some(keys) = self.by_target_isolate_id.get_mut(&target_isolate_id) {
                    keys.remove(&key);
                    keys.is_empty()
                } else {
                    false
                };
            if remove_target_entry {
                self.by_target_isolate_id.remove(&target_isolate_id);
            }
        }
        if let Some(memory_route) = pending.memory_route.as_ref() {
            let remove_memory_entry =
                if let Some(keys) = self.by_memory_owner_key.get_mut(&memory_route.owner_key) {
                    keys.remove(&key);
                    keys.is_empty()
                } else {
                    false
                };
            if remove_memory_entry {
                self.by_memory_owner_key.remove(&memory_route.owner_key);
            }
        }
        let remove_enqueued_entry =
            if let Some(keys) = self.by_enqueued_at.get_mut(&pending.enqueued_at) {
                keys.remove(&key);
                keys.is_empty()
            } else {
                false
            };
        if remove_enqueued_entry {
            self.by_enqueued_at.remove(&pending.enqueued_at);
        }
    }

    #[cfg(test)]
    pub(super) fn indexed_target_count(&self, target_isolate_id: u64) -> usize {
        self.by_target_isolate_id
            .get(&target_isolate_id)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn indexed_memory_count(&self, owner_key: &str) -> usize {
        self.by_memory_owner_key
            .get(owner_key)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn indexed_enqueued_count(&self) -> usize {
        self.by_enqueued_at
            .values()
            .map(HashSet::len)
            .sum::<usize>()
    }

    fn lane(&self, lane: PendingQueueLane) -> &BTreeMap<u64, PendingInvoke> {
        match lane {
            PendingQueueLane::Targeted => &self.targeted,
            PendingQueueLane::Memory => {
                panic!("memory queue is sharded; iterate memory_shards instead")
            }
            PendingQueueLane::General => &self.general,
        }
    }

    fn find_lane_oldest_map<T>(
        &self,
        lane: PendingQueueLane,
        map: &mut impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<(PendingQueueKey, u64, T)> {
        let mut selected = None;
        for (sequence, pending) in self.lane(lane) {
            let key = PendingQueueKey {
                lane,
                sequence: *sequence,
                memory_shard_index: None,
                memory_owner_key: None,
            };
            let Some(value) = map(key.clone(), pending) else {
                continue;
            };
            selected = Some((key, *sequence, value));
            break;
        }
        selected
    }

    fn find_memory_round_robin_head_map<T>(
        &mut self,
        map: &mut impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<(PendingQueueKey, u64, T)> {
        if self.memory_shards.is_empty() {
            return None;
        }

        let cursor = self.memory_next_shard_cursor.unwrap_or(0);
        let active_shards = self.memory_shards.len();
        let mut wrapped = false;
        let mut last_seen = None;
        for _ in 0..active_shards {
            let shard_index = if let Some(last_seen) = last_seen {
                self.memory_shards
                    .range((Excluded(last_seen), Unbounded))
                    .next()
                    .map(|(shard_index, _)| *shard_index)
                    .or_else(|| {
                        if wrapped {
                            None
                        } else {
                            wrapped = true;
                            self.memory_shards.keys().next().copied()
                        }
                    })?
            } else {
                self.memory_shards
                    .range(cursor..)
                    .next()
                    .map(|(shard_index, _)| *shard_index)
                    .or_else(|| {
                        wrapped = true;
                        self.memory_shards.keys().next().copied()
                    })?
            };
            last_seen = Some(shard_index);
            let Some(memory_shard) = self.memory_shards.get_mut(&shard_index) else {
                continue;
            };
            let Some(candidate) = memory_shard.find_round_robin_owner_head_map(shard_index, map)
            else {
                continue;
            };
            return Some(candidate);
        }
        None
    }

    fn advance_memory_cursor(&mut self, key: &PendingQueueKey) {
        let Some(shard_index) = key.memory_shard_index else {
            return;
        };
        if let (Some(memory_shard), Some(owner_key)) = (
            self.memory_shards.get_mut(&shard_index),
            key.memory_owner_key.as_deref(),
        ) {
            memory_shard.advance_owner_cursor(owner_key);
        }
        self.memory_next_shard_cursor = self
            .memory_shards
            .range((shard_index.saturating_add(1))..)
            .next()
            .map(|(next_shard, _)| *next_shard)
            .or_else(|| self.memory_shards.keys().next().copied());
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::service) struct PendingQueueLaneDepths {
    pub(in crate::service) targeted: usize,
    pub(in crate::service) memory: usize,
    pub(in crate::service) general: usize,
}
