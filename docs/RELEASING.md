# Releasing

Releases are triggered manually by pushing an annotated version tag (`v<VERSION>`). The [release workflow](../.github/workflows/release.yml) runs CI, builds cross-platform archives, verifies checksums, publishes the GitHub Release, and triggers the GHCR container build.

Only stable semantic versions (such as `0.1.1` or `0.2.0`) are accepted; pre-release tags like `v0.2.0-rc.1` are rejected. `VERSION` is the single source of truth, and the Git tag must match `v$(cat VERSION)`.

## Prepare a release

1. Update `VERSION` (patch bump for bug fixes, minor bump for features while on `0.x`).
2. Merge to `main`, wait for CI to pass, and ensure your local `main` checkout is clean and up to date.
3. Create an annotated tag matching `VERSION`:

   ```bash
   git tag -a v0.1.1 -m "Release v0.1.1"
   ```

## Preview locally

Requires Go, Git, Make, and Python 3.9+ on Linux or macOS:

```bash
make release TAG=v0.1.1
```

This builds `dist/v0.1.1/` locally without pushing or uploading anything:
- Four archives (`linux`/`darwin` × `amd64`/`arm64`), each containing `steady-proxy`, `VERSION`, and `LICENSE`.
- `SHA256SUMS` for all four archives.

The packager verifies that `TAG` points to `HEAD`, the working tree is clean, and the host binary's `--version` output matches the tag metadata. It refuses to overwrite an existing `dist/<TAG>` directory (remove `dist/v0.1.1` first if re-running). Run `make release-test` to execute the packaging test suite.

## Publish

Push the tag to trigger the release workflow:

```bash
git push origin v0.1.1
```

The workflow:
1. Runs the full CI suite (`gofmt`, `go vet`, `go build`, `go test -race`, `make release-test`, and Docker build).
2. Builds the release archives and uploads them as a 7-day workflow artifact.
3. Verifies `SHA256SUMS` in a release-publishing job, refuses to overwrite any existing release or draft for that tag, creates a draft release with generated release notes, and publishes it once all assets upload.
4. Invokes the container workflow to build and push `ghcr.io/xllent-ai/steady-proxy:v0.1.1`.

All steps use GitHub's built-in `GITHUB_TOKEN`; no external secrets are required.

## Container images

The [container workflow](../.github/workflows/docker-publish.yml) publishes multi-arch Linux `amd64` and `arm64` images to `ghcr.io/xllent-ai/steady-proxy`:

- **Push to `main`:** Publishes `latest` and `sha-<full-commit-sha>`.
- **Release published:** Publishes `<release-tag>` (e.g., `v0.1.1`) and `sha-<full-commit-sha>`. Release builds never overwrite `latest`.

Note: Because releases created with `GITHUB_TOKEN` do not fire `release` workflow events, `release.yml` calls `docker-publish.yml` directly via `workflow_call`. Manually published GitHub releases trigger `docker-publish.yml` via `release: published`.

After the first package push, verify in GitHub package settings that `ghcr.io/xllent-ai/steady-proxy` visibility is set to **Public** so unauthenticated pulls work.

## If a release fails

- **CI or packaging failed:** No GitHub release was created. Fix the issue, bump to a new version if commits changed, or re-run if transient.
- **Asset upload failed mid-publish:** Delete the leftover draft release in GitHub before re-running the failed job (the workflow refuses to overwrite existing drafts).
- **Container job failed after release published:** Re-run only the failed container job in GitHub Actions; do not delete or recreate the published release.
- **Bug found after publishing:** Never move or overwrite a published tag. Cut a new patch version in `VERSION` and tag a new release.
