"""Exercise release boundaries and packaging without publishing or cross-compiling."""

from contextlib import redirect_stdout
import hashlib
import io
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from scripts import package_release as release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="steady-release-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "VERSION").write_text("0.1.1\n")
        (self.root / "LICENSE").write_text("Test license\n")
        (self.root / ".gitignore").write_text("/dist/\n")
        self.git("init", "--quiet")
        # Temporary repositories must not leave housekeeping running during cleanup.
        self.git("config", "maintenance.auto", "false")
        self.git("config", "gc.auto", "0")
        self.commit()
        self.git("tag", "v0.1.1")
        self.build = patch.object(release, "build_binary", side_effect=self.fake_build).start()
        self.addCleanup(patch.stopall)

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()

    def commit(self):
        self.git("add", ".")
        self.git(
            "-c", "user.name=Release tests", "-c", "user.email=release-tests@example.invalid",
            "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Test fixture",
        )

    def fake_build(self, root, binary, target, metadata):
        # Stand in for the compiler; the real four-platform build is checked separately.
        output = (
            f"steady-proxy version={metadata.version} commit={metadata.commit} "
            f"build_date={metadata.build_date} dirty=false"
        )
        binary.write_text("#!/bin/sh\nprintf '%s\\n' " + shlex.quote(output) + "\n")
        binary.chmod(0o755)

    def package(self, tag="v0.1.1"):
        with redirect_stdout(io.StringIO()):
            return release.package_release(self.root, tag)

    def assert_no_partial_output(self):
        self.assertFalse((self.root / "dist" / "v0.1.1").exists())
        self.assertEqual(list((self.root / "dist").glob(".release-*")), [])

    def test_archives_contain_only_release_files_and_valid_checksums(self):
        output = self.package()
        expected = {
            f"steady-proxy_0.1.1_{target}.tar.gz"
            for target in ("linux_amd64", "linux_arm64", "darwin_amd64", "darwin_arm64")
        }
        self.assertEqual({path.name for path in output.iterdir()}, expected | {"SHA256SUMS"})
        recorded = {}
        for line in (output / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ")
            recorded[name] = digest
        self.assertEqual(set(recorded), expected)
        for name, digest in recorded.items():
            archive = output / name
            self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), digest)
            prefix = name.removesuffix(".tar.gz")
            with tarfile.open(archive) as tar:
                self.assertEqual(
                    {member.name for member in tar if member.isfile()},
                    {f"{prefix}/{file}" for file in ("steady-proxy", "VERSION", "LICENSE")},
                )
                self.assertEqual(tar.extractfile(f"{prefix}/VERSION").read(), b"0.1.1\n")
                self.assertEqual(tar.extractfile(f"{prefix}/LICENSE").read(), b"Test license\n")
                self.assertTrue(tar.getmember(f"{prefix}/steady-proxy").mode & 0o111)
        self.assertEqual(list(output.parent.glob(".release-*")), [])

    def test_tag_must_match_version(self):
        with self.assertRaisesRegex(release.ReleaseError, "must match VERSION"):
            self.package("v0.1.2")
        self.build.assert_not_called()

    def test_version_must_be_stable_semver(self):
        for version in ("0.1", "01.2.3", "0.1.1-rc.1", "../escape", "0.1.1\nextra"):
            with self.subTest(version=version):
                (self.root / "VERSION").write_text(version)
                with self.assertRaisesRegex(release.ReleaseError, "stable version"):
                    self.package("v" + version)
        self.build.assert_not_called()

    def test_tag_must_point_to_checkout(self):
        (self.root / "LICENSE").write_text("Changed license\n")
        self.commit()
        with self.assertRaisesRegex(release.ReleaseError, "checked-out commit"):
            self.package()
        self.build.assert_not_called()

    def test_dirty_and_untracked_files_are_rejected(self):
        for name in ("LICENSE", "untracked.txt"):
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_bytes() if path.exists() else None
                path.write_text("Uncommitted change\n")
                with self.assertRaisesRegex(release.ReleaseError, "clean checkout"):
                    self.package()
                if original is None:
                    path.unlink()
                else:
                    path.write_bytes(original)
        self.build.assert_not_called()

    def test_existing_output_is_preserved(self):
        output = self.root / "dist" / "v0.1.1"
        output.mkdir(parents=True)
        sentinel = output / "existing-asset"
        sentinel.write_text("Do not replace\n")
        with self.assertRaisesRegex(release.ReleaseError, "refusing to overwrite"):
            self.package()
        self.assertEqual(sentinel.read_text(), "Do not replace\n")
        self.build.assert_not_called()

    def test_failed_build_leaves_no_partial_release(self):
        def fail_second_build(root, binary, target, metadata):
            if target == ("linux", "arm64"):
                raise subprocess.CalledProcessError(2, ["make", "build"])
            self.fake_build(root, binary, target, metadata)

        self.build.side_effect = fail_second_build
        with self.assertRaises(subprocess.CalledProcessError):
            self.package()
        self.assert_no_partial_output()

    def test_incorrect_binary_metadata_is_rejected(self):
        def wrong_version(root, binary, target, metadata):
            binary.write_text("#!/bin/sh\nprintf 'steady-proxy version=wrong\\n'\n")
            binary.chmod(0o755)

        self.build.side_effect = wrong_version
        with self.assertRaisesRegex(release.ReleaseError, "metadata mismatch"):
            self.package()
        self.assert_no_partial_output()

    def test_checkout_changes_during_build_are_rejected(self):
        def change_checkout(root, binary, target, metadata):
            self.fake_build(root, binary, target, metadata)
            (root / "LICENSE").write_text("Changed during build\n")

        self.build.side_effect = change_checkout
        with self.assertRaisesRegex(release.ReleaseError, "clean checkout"):
            self.package()
        self.assert_no_partial_output()


if __name__ == "__main__":
    unittest.main()
