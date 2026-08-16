# Releasing

A release is a pushed version tag. There is no manual dispatch — CI runs
the full gate and publishes the GitHub Release only if everything passes.

## Why tags

The Go module proxy resolves version tags directly: the tag name **is**
the `go get` version. A tag of `v1.2.3` makes
`go get github.com/go-warehouse/events@v1.2.3` work. For that reason the
tag shape is validated strictly before anything is published.

## Cutting a release

1. Make sure `main` is green — CI runs the same gates on every push, so a
   release can never pass what a commit failed.
2. Tag and push:

   ```sh
   git tag v1.2.3
   git push origin v1.2.3
   ```

3. Watch the `release` workflow (Actions → release). It runs the gate,
   then creates the GitHub Release named after the tag.

Done. The release notes are generated from the commit prefixes — write
good `feat:` / `fix:` / `docs:` messages. The grouping lives in
[.goreleaser.yml](.goreleaser.yml).

## Tag rules

- `vX.Y.Z` for a stable release, or `vX.Y.Z-rcN` for a release candidate
  (published as a prerelease).
- Anything else is ignored by the trigger pattern, and the validate step
  rejects shapes that slip through the pattern before publishing.
- Never move or re-tag a pushed tag — the module proxy caches it.

## What runs on a tag

The `release` workflow is the only one with `contents: write`. It runs,
in order:

1. **quality** (reusable workflow, shared with CI)
   - `go vet ./...`
   - `go test -race -failfast ./...`
   - `make coverage` — the same ≥ 80% floor enforced locally
   - golangci-lint, pinned to the version in CI
2. **build** (reusable workflow, shared with CI) — cross-compile matrix:
   linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64
3. **create release** — goreleaser (pinned to the version in CI) generates
   the changelog from conventional commits since the previous tag, groups
   them by prefix (Features / Bug fixes / Documentation), and publishes
   the GitHub Release.

## Versioning

While the API may still change, stay on `v0.x.y`. Once stable, follow
semver — and remember Go's module rule: a `v2+` release must live at a
module path ending in `/v2` (e.g. `github.com/go-warehouse/events/v2`).
