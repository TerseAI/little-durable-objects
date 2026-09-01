# Releasing

A stable GitHub Release publishes three artifacts from the tagged commit:

- `ghcr.io/terseai/little-durable-objects:<version>` and `latest`, as a source for the Rust runtime binary
- `little-durable-objects` on npm
- `little-durable-objects` on crates.io

Before publishing, the workflow requires a stable `vX.Y.Z` tag on `main` and matching package manifests. It runs both test suites and verifies the npm and Cargo archives.

The GHCR artifact intentionally contains only the runtime and its native libraries. A Terse sandbox image should copy `/usr/local/bin/little-durable-objects` from it, then install the matching `little-durable-objects` version into the customer project so the host can load its JavaScript executor.

## One-time setup

1. Log in to npm with an account using two-factor authentication and publish `little-durable-objects@0.1.0` once from the `npm` directory with `npm publish --access public`.
2. Create a crates.io API token allowed to publish a new crate and add it as the `CARGO_REGISTRY_TOKEN` GitHub Actions secret.
3. Confirm that GitHub Actions may write organization packages so `GITHUB_TOKEN` can create `ghcr.io/terseai/little-durable-objects`.

After the first npm publish, configure its trusted publisher on npmjs.com:

- Provider: GitHub Actions
- Organization: `TerseAI`
- Repository: `little-durable-objects`
- Workflow filename: `release.yml`
- Allowed action: `npm publish`

Later npm releases use short-lived OIDC credentials and receive automatic provenance attestations. Replace the crates.io token with one scoped to `little-durable-objects`, and make the new GHCR package public in its GitHub package settings.

## Publishing a release

Prepare the next stable version:

```sh
pnpm release:prepare 0.1.1
pnpm test
git add Cargo.toml Cargo.lock npm/package.json
git commit -m "Release v0.1.1"
git push origin main
```

Once CI passes on that commit, publish a GitHub Release:

```sh
gh release create v0.1.1 --target main --title v0.1.1 --generate-notes --fail-on-no-commits
```

For the initial `v0.1.0` release, the manifests are already stamped; commit and merge the publication setup, then create the release without running `release:prepare`.

If a registry is temporarily unavailable, rerun the failed workflow jobs. Image publication is replaceable, and both registry jobs skip versions that already exist.
