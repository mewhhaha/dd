set shell := ["bash", "-euo", "pipefail", "-c"]

# Copy a crates.io dependency into vendor/<crate> so we can patch it locally.
patch crate version='':
  ./scripts/patch-crate.sh {{crate}} {{version}}

# Replace an existing vendored crate with a fresh copy from the local cargo registry cache.
patch-refresh crate version='':
  PATCH_REFRESH=1 ./scripts/patch-crate.sh {{crate}} {{version}}
