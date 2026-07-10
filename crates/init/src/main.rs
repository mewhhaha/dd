#[cfg(unix)]
mod unix {
    use std::env;
    use std::ffi::{OsStr, OsString};
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Write};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, lchown};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    const STORE_DIR: &str = "/app/store";
    const DEFAULT_SERVER: &str = "/app/dd_server";
    const MARKER_FILE: &str = ".dd-volume-ownership-v1";
    const TARGET_UID: libc::uid_t = 65_532;
    const TARGET_GID: libc::gid_t = 65_532;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RepairOutcome {
        Repaired,
        AlreadyCurrent,
    }

    pub(super) fn run() -> io::Result<()> {
        let store_dir = Path::new(STORE_DIR);
        let outcome = repair_store_ownership(store_dir, TARGET_UID, TARGET_GID)?;
        match outcome {
            RepairOutcome::Repaired => {
                eprintln!("dd_init: repaired {STORE_DIR} ownership for {TARGET_UID}:{TARGET_GID}")
            }
            RepairOutcome::AlreadyCurrent => {
                eprintln!("dd_init: ownership marker is current; skipping repair")
            }
        }

        drop_privileges(TARGET_UID, TARGET_GID)?;
        exec_server(env::args_os().skip(1).collect())
    }

    fn repair_store_ownership(
        store_dir: &Path,
        uid: libc::uid_t,
        gid: libc::gid_t,
    ) -> io::Result<RepairOutcome> {
        fs::create_dir_all(store_dir)?;
        if !fs::symlink_metadata(store_dir)?.file_type().is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "container store path must be a directory, not a symlink",
            ));
        }
        let marker = store_dir.join(MARKER_FILE);
        if marker_is_current(&marker, uid, gid)? {
            return Ok(RepairOutcome::AlreadyCurrent);
        }

        require_repair_permissions(uid, gid)?;
        repair_tree(store_dir, uid, gid)?;
        write_marker(store_dir, &marker, uid, gid)?;
        Ok(RepairOutcome::Repaired)
    }

    fn marker_is_current(marker: &Path, uid: libc::uid_t, gid: libc::gid_t) -> io::Result<bool> {
        let expected = marker_contents(uid, gid);
        let store_metadata = fs::symlink_metadata(
            marker
                .parent()
                .ok_or_else(|| io::Error::other("ownership marker has no parent directory"))?,
        )?;
        if !store_metadata.file_type().is_dir()
            || store_metadata.uid() != uid
            || store_metadata.gid() != gid
        {
            return Ok(false);
        }
        let metadata = match fs::symlink_metadata(marker) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if !metadata.file_type().is_file()
            || metadata.uid() != uid
            || metadata.gid() != gid
            || metadata.len() != expected.len() as u64
        {
            return Ok(false);
        }
        Ok(fs::read(marker)? == expected.as_bytes())
    }

    fn require_repair_permissions(uid: libc::uid_t, gid: libc::gid_t) -> io::Result<()> {
        if effective_uid() == 0 || (effective_uid() == uid && effective_gid() == gid) {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "volume ownership marker is missing or invalid and dd_init cannot repair ownership",
        ))
    }

    fn repair_tree(root: &Path, uid: libc::uid_t, gid: libc::gid_t) -> io::Result<()> {
        let mut pending = vec![root.to_path_buf()];
        while let Some(path) = pending.pop() {
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_dir() {
                for entry in fs::read_dir(&path)? {
                    pending.push(entry?.path());
                }
            }
            lchown(&path, Some(uid), Some(gid))?;
        }
        Ok(())
    }

    fn write_marker(
        store_dir: &Path,
        marker: &Path,
        uid: libc::uid_t,
        gid: libc::gid_t,
    ) -> io::Result<()> {
        let temporary = store_dir.join(format!("{MARKER_FILE}.tmp-{}", std::process::id()));
        let result = (|| {
            match fs::remove_file(&temporary) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(marker_contents(uid, gid).as_bytes())?;
            file.sync_all()?;
            lchown(&temporary, Some(uid), Some(gid))?;
            fs::rename(&temporary, marker)?;
            File::open(store_dir)?.sync_all()
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn marker_contents(uid: libc::uid_t, gid: libc::gid_t) -> String {
        format!("dd-volume-ownership\nversion=1\nuid={uid}\ngid={gid}\n")
    }

    fn drop_privileges(uid: libc::uid_t, gid: libc::gid_t) -> io::Result<()> {
        let current_uid = effective_uid();
        let current_gid = effective_gid();
        if current_uid != 0 {
            if current_uid == uid && current_gid == gid {
                return Ok(());
            }
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "dd_init must run as root or {uid}:{gid}, currently {current_uid}:{current_gid}"
                ),
            ));
        }

        // SAFETY: these calls mutate only this single-threaded init process, and
        // every return value is checked before the server is executed.
        unsafe {
            if libc::setgroups(0, std::ptr::null()) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::setgid(gid) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::setuid(uid) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        if effective_uid() != uid || effective_gid() != gid {
            return Err(io::Error::other("failed to drop container privileges"));
        }
        Ok(())
    }

    fn exec_server(mut args: Vec<OsString>) -> io::Result<()> {
        if args.first().is_some_and(|arg| arg == OsStr::new("--")) {
            args.remove(0);
        }
        let command = if args.is_empty() {
            OsString::from(DEFAULT_SERVER)
        } else {
            args.remove(0)
        };
        eprintln!(
            "dd_init: executing {} as {TARGET_UID}:{TARGET_GID}",
            PathBuf::from(&command).display()
        );
        let error = Command::new(command).args(args).exec();
        Err(error)
    }

    fn effective_uid() -> libc::uid_t {
        // SAFETY: geteuid has no preconditions.
        unsafe { libc::geteuid() }
    }

    fn effective_gid() -> libc::gid_t {
        // SAFETY: getegid has no preconditions.
        unsafe { libc::getegid() }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

        fn temp_dir(label: &str) -> PathBuf {
            env::temp_dir().join(format!(
                "dd-init-{label}-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ))
        }

        #[test]
        fn ownership_marker_makes_repair_one_time() {
            let root = temp_dir("marker");
            fs::create_dir_all(root.join("nested")).expect("create test store");
            fs::write(root.join("nested/value"), b"persisted").expect("write test value");
            let uid = effective_uid();
            let gid = effective_gid();

            assert_eq!(
                repair_store_ownership(&root, uid, gid).expect("initial repair"),
                RepairOutcome::Repaired
            );
            assert_eq!(
                repair_store_ownership(&root, uid, gid).expect("repeat repair"),
                RepairOutcome::AlreadyCurrent
            );
            assert_eq!(
                fs::read_to_string(root.join(MARKER_FILE)).expect("read marker"),
                marker_contents(uid, gid)
            );

            fs::remove_dir_all(root).expect("remove test store");
        }

        #[test]
        fn invalid_marker_is_replaced() {
            let root = temp_dir("invalid-marker");
            fs::create_dir_all(&root).expect("create test store");
            fs::write(root.join(MARKER_FILE), b"old marker").expect("write old marker");
            let uid = effective_uid();
            let gid = effective_gid();

            assert_eq!(
                repair_store_ownership(&root, uid, gid).expect("repair invalid marker"),
                RepairOutcome::Repaired
            );
            assert!(marker_is_current(&root.join(MARKER_FILE), uid, gid).expect("check marker"));

            fs::remove_dir_all(root).expect("remove test store");
        }

        #[test]
        fn ownership_repair_does_not_follow_symlinks() {
            use std::os::unix::fs::symlink;

            let root = temp_dir("symlink");
            let outside = temp_dir("outside");
            fs::create_dir_all(&root).expect("create test store");
            fs::create_dir_all(&outside).expect("create outside directory");
            fs::write(outside.join("value"), b"outside").expect("write outside value");
            symlink(&outside, root.join("outside-link")).expect("create symlink");

            repair_store_ownership(&root, effective_uid(), effective_gid())
                .expect("repair symlink tree");
            assert_eq!(
                fs::read(outside.join("value")).expect("read outside value"),
                b"outside"
            );

            fs::remove_dir_all(root).expect("remove test store");
            fs::remove_dir_all(outside).expect("remove outside directory");
        }

        #[test]
        fn store_root_symlink_is_rejected() {
            use std::os::unix::fs::symlink;

            let link = temp_dir("root-link");
            let target = temp_dir("root-target");
            fs::create_dir_all(&target).expect("create symlink target");
            symlink(&target, &link).expect("create store symlink");

            let error = repair_store_ownership(&link, effective_uid(), effective_gid())
                .expect_err("store symlink should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

            fs::remove_file(link).expect("remove store symlink");
            fs::remove_dir_all(target).expect("remove symlink target");
        }
    }
}

#[cfg(unix)]
fn main() {
    if let Err(error) = unix::run() {
        eprintln!("dd_init: {error}");
        std::process::exit(1);
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("dd_init is only supported on Unix containers");
    std::process::exit(1);
}
