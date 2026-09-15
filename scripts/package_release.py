#!/usr/bin/env python3
"""Build local release archives from a clean, tagged checkout. Never publishes."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile


TARGETS = (("linux", "amd64"), ("linux", "arm64"), ("darwin", "amd64"), ("darwin", "arm64"))
VERSION_PATTERN = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"


class ReleaseError(Exception):
    pass


@dataclass(frozen=True)
class Release:
    version: str
    commit: str
    build_date: str


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def validate_source(root, tag):
    version = (root / "VERSION").read_text().strip()
    if not re.fullmatch(VERSION_PATTERN, version):
        raise ReleaseError("VERSION must contain a stable version such as 0.1.1")
    if tag != "v" + version:
        raise ReleaseError(f"release tag must match VERSION: expected v{version}, got {tag!r}")
    commit = git(root, "rev-parse", "HEAD")
    if git(root, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}") != commit:
        raise ReleaseError(f"{tag} must point to the checked-out commit")
    if git(root, "status", "--porcelain", "--untracked-files=normal"):
        raise ReleaseError("release packaging requires a clean checkout, including untracked files")
    return version, commit


def build_binary(root, binary, target, release):
    target_os, target_arch = target
    env = dict(os.environ, GOOS=target_os, GOARCH=target_arch)
    subprocess.run(
        [
            "make", "build", f"BINARY={binary}", f"VERSION={release.version}",
            f"COMMIT={release.commit}", f"BUILD_DATE={release.build_date}", "DIRTY=false",
        ],
        cwd=root, env=env, check=True,
    )


def archive_metadata(info):
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


def package_release(root, tag):
    version, commit = validate_source(root, tag)
    release = Release(version, commit, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    output = root / "dist" / tag
    if output.exists():
        raise ReleaseError(f"refusing to overwrite existing output: {output}")
    host = tuple(subprocess.check_output(["go", "env", "GOHOSTOS", "GOHOSTARCH"], cwd=root, text=True).split())
    output.parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".release-", dir=output.parent) as temporary:
        work = Path(temporary)
        assets = work / "assets"
        assets.mkdir()
        checksums = []
        for target in TARGETS:
            name = f"steady-proxy_{version}_{target[0]}_{target[1]}"
            package = work / name
            package.mkdir()
            binary = package / "steady-proxy"
            build_binary(root, binary, target, release)
            if target == host:
                actual = subprocess.check_output([str(binary), "--version"], text=True).strip()
                expected = (
                    f"steady-proxy version={version} commit={commit} "
                    f"build_date={release.build_date} dirty=false"
                )
                if actual != expected:
                    raise ReleaseError(f"binary version metadata mismatch: {actual!r}")
            shutil.copyfile(root / "LICENSE", package / "LICENSE")
            shutil.copyfile(root / "VERSION", package / "VERSION")
            archive = assets / f"{name}.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(package, arcname=name, filter=archive_metadata)
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            checksums.append(f"{digest}  {archive.name}\n")
            print(f"Packaged {archive.name}", flush=True)
        (assets / "SHA256SUMS").write_text("".join(sorted(checksums)))
        # Do not label artifacts clean if the checkout changed during the build.
        if validate_source(root, tag) != (version, commit):
            raise ReleaseError("release source changed during packaging")
        if output.exists():
            raise ReleaseError(f"refusing to overwrite existing output: {output}")
        assets.rename(output)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="existing stable tag matching VERSION, such as v0.1.1")
    args = parser.parse_args()
    try:
        output = package_release(Path(__file__).resolve().parents[1], args.tag)
    except (ReleaseError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"release packaging failed: {error}\n")
    print(f"Release archives are ready in {output}; nothing was uploaded.")


if __name__ == "__main__":
    main()
