set shell := ["bash", "-euo", "pipefail", "-c"]

# Materialize vendor/<crate> from the locked crates.io source and apply patches/<crate>.patch if it exists.
patch crate version='':
  ./scripts/patch-crate.sh {{crate}} {{version}}

# Replace an existing vendored crate from the local cargo registry cache, then apply patches/<crate>.patch.
patch-refresh crate version='':
  PATCH_REFRESH=1 ./scripts/patch-crate.sh {{crate}} {{version}}

# Regenerate patches/<crate>.patch from vendor/<crate> versus the locked crates.io source.
patch-save crate version='':
  ./scripts/patch-save-crate.sh {{crate}} {{version}}
