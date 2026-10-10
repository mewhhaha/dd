//! Runs under a global allocator that refuses any single Rust allocation over
//! 256 MiB, so converting a value whose claimed length JavaScript picked for
//! free (a sparse array's `length`, a length field in serialized bytes) and
//! reserving memory for that claim aborts the test instead of passing on a
//! machine that overcommits.

mod common;

use common::{assert_checks, runtime};
use std::alloc::{GlobalAlloc, Layout, System};

const LIMIT: usize = 256 << 20;

struct Capped;

// SAFETY: forwards to `System`, refusing (with a null pointer, as the trait
// allows) only requests over `LIMIT`.
unsafe impl GlobalAlloc for Capped {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() > LIMIT {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() > LIMIT {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > LIMIT {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Capped = Capped;

/// `JsBuffer` used to reserve a JavaScript array's full `length` before
/// reading its first element, so `new Array(2 ** 32 - 1)` asked for 4 GiB.
#[test]
fn sparse_array_lengths_reserve_nothing_up_front() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const sparse = new Array(2 ** 32 - 1);
        const ending = new Array(2 ** 32 - 1);
        ending[0] = 1;
        ending[1] = 2;
        for (const [label, call] of [
          ["JsBuffer", (v) => ops.op_echo_bytes(v)],
          ["Vec<u8>", (v) => ops.op_echo_vec(v)],
          ["Option<JsBuffer>", (v) => ops.op_echo_optional(v)],
          ["struct field", (v) => ops.op_echo_field({ data: v })],
          ["Vec<String>", (v) => ops.op_strings(v)],
        ]) {
          for (const value of [sparse, ending]) {
            const result = attempt(() => call(value));
            check(result.error instanceof TypeError, `${label}: ${describe(result.error ?? result.value)}`);
          }
        }
        "#,
    );
}

/// Length fields inside serialized bytes are checked against the bytes that
/// are actually there before anything is allocated.
#[test]
fn forged_lengths_in_serialized_bytes_allocate_nothing() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const varint = (n) => {
          const out = [];
          n = BigInt(n);
          do {
            let byte = Number(n & 0x7fn);
            n >>= 7n;
            if (n) byte |= 0x80;
            out.push(byte);
          } while (n);
          return out;
        };
        const huge = [2 ** 31, 2 ** 32 - 1, 2n ** 53n, 2n ** 64n - 1n];
        // string, two-byte string, ArrayBuffer, dense array, BigInt digits,
        // UTF-8 string, resizable ArrayBuffer byte length.
        for (const tag of [0x22, 0x63, 0x42, 0x41, 0x5a, 0x53, 0x7e]) {
          for (const length of huge) {
            const bytes = new Uint8Array([0xff, 0x0f, tag, ...varint(length), 1, 2, 3]);
            for (const forStorage of [false, true]) {
              const result = attempt(() => ops.op_deserialize(bytes, undefined, undefined, undefined, forStorage));
              check(!result.error || isCleanError(result.error), `tag ${tag} length ${length}: ${describe(result.error)}`);
            }
          }
        }
        "#,
    );
}
