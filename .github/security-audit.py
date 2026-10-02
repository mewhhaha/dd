"""Run cargo-audit with the reviewed, version-scoped exceptions in SECURITY.md."""

import datetime
import pathlib
import re
import subprocess
import sys
import tomllib


def reviewed_exceptions(policy, lockfile, today):
    packages = {
        (package["name"], package["version"])
        for package in tomllib.loads(lockfile.read_text())["package"]
    }
    exceptions = []
    for line in policy.read_text().splitlines():
        if not line.startswith("| [RUSTSEC-"):
            continue
        columns = [column.strip() for column in line.split("|")[1:-1]]
        if len(columns) != 5:
            raise ValueError("security exception rows require five columns")
        advisory, package, reviewed, expires, owner = columns
        match = re.fullmatch(
            r"\[(RUSTSEC-\d{4}-\d{4})\]\(https://rustsec\.org/advisories/\1\.html\)",
            advisory,
        )
        if match is None or not owner:
            raise ValueError(f"invalid security exception: {line}")
        advisory_id = match[1]
        reviewed = datetime.date.fromisoformat(reviewed)
        expires = datetime.date.fromisoformat(expires)
        if reviewed > today or expires <= today:
            raise ValueError(f"{advisory_id}: review is in the future or exception expired")
        if not 0 < (expires - reviewed).days <= 90:
            raise ValueError(f"{advisory_id}: review period must be between 1 and 90 days")
        name, version = package.strip("`").rsplit(" ", 1)
        if {actual for package_name, actual in packages if package_name == name} != {version}:
            raise ValueError(f"{advisory_id}: {name} versions changed; reassess the exception")
        if advisory_id in exceptions:
            raise ValueError(f"duplicate security exception: {advisory_id}")
        exceptions.append(advisory_id)
    return exceptions


def main():
    project = pathlib.Path(__file__).resolve().parents[1]
    today = datetime.datetime.now(datetime.timezone.utc).date()
    exceptions = reviewed_exceptions(project / "SECURITY.md", project / "Cargo.lock", today)
    if {"RUSTSEC-2026-0118", "RUSTSEC-2026-0119"}.intersection(exceptions):
        features = subprocess.check_output(
            ["cargo", "tree", "--locked", "--all-features", "-e", "features", "-i", "hickory-proto"],
            cwd=project,
            text=True,
        )
        if re.search(r'hickory-proto feature "(?:__dnssec|dnssec-[^"]+)"', features):
            raise ValueError("Hickory DNSSEC was enabled; its security exceptions need reassessment")
    command = ["cargo", "audit"]
    for advisory in exceptions:
        command.extend(["--ignore", advisory])
    return subprocess.call(command, cwd=project)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValueError as error:
        print(f"security exception policy: {error}", file=sys.stderr)
        sys.exit(1)
