# Releasing

Choose the version and release timing manually. Pushing a matching version tag
starts the [release workflow](../.github/workflows/release.yml), which runs CI,
builds archives, generates checksums and release notes, and publishes to GitHub.

The workflow supports stable versions such as `0.1.1` and `0.2.0`. Release-candidate
tags such as `v0.2.0-rc.1` are rejected. `VERSION` is the source of truth; the tag
must be exactly `v` followed by that value.

## Prepare a release

1. Update `VERSION`. Use a patch increment for fixes, or a minor increment for new
   features while the project is at `0.x`.
2. Commit and merge the change into `main`. Wait for CI to pass, and check that
   your local checkout contains the intended commit and has no uncommitted files.
3. Create an annotated tag. For example, after changing `VERSION` to `0.1.1`:

   ```bash
   git tag -a v0.1.1 -m "Release v0.1.1"
   ```

## Preview locally

Local packaging needs Go, Git, Make, and Python 3.9 or later on Linux or macOS.
Run it from the clean, tagged checkout:

```bash
make release TAG=v0.1.1
```

This only creates files under `dist/v0.1.1/`; it does not push the tag, contact
GitHub, or upload anything. The directory contains four archives for Linux and
macOS on amd64 and arm64, plus `SHA256SUMS`. Each archive includes the executable,
`VERSION`, and the MIT license. The executable reports its version, commit, build
time, and clean working-tree status.

The packager checks that the tag points to the current commit and that the
checkout is clean. It also runs `--version` for the host platform's binary to
check its metadata. It refuses to replace an existing output directory. Failed
builds clean up their temporary files without leaving a completed release folder.

The output directory is ignored by Git. To package the same tag again, move or
remove only that tag's local output directory first. Packaging does not replace
the test suite; `make release-test` checks packaging failures and archive contents,
while normal CI checks the application and Docker build.

## Publish

When the version, code, and local preview are ready, push the tag:

```bash
git push origin v0.1.1
```

The workflow then:

1. Runs the existing CI workflow, including formatting, vet, build, race tests,
   packaging tests, and the Docker build.
2. Packages the tagged source and retains the archives as a workflow artifact for
   seven days.
3. Verifies the downloaded archives against `SHA256SUMS` in a separate job with
   permission to create releases.
4. Refuses to proceed if any release already uses the tag, including a draft.
5. Creates a draft with generated GitHub release notes and all five assets, then
   publishes it after uploading succeeds.

The workflow uses GitHub's automatic token; no personal token is needed. It does
not upload container images. Review the published release notes for any migration
or configuration guidance that commit and pull-request summaries do not explain.

## If a release fails

Use the failed Actions run to identify the failing step. A test or packaging
failure creates no GitHub release. An upload or publication failure can leave a
draft; inspect it and delete that draft before rerunning the failed jobs. The
workflow will not overwrite its assets automatically.

If a published release needs a fix, use a new version and tag. Keep existing
published tags and assets unchanged so downloads continue to identify the same
code and checksums.
