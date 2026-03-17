# Athanor Registry

Official registry for [Open Athanor](https://github.com/alembic-ai/opn_athnr_beta) modules, skills, and stack blueprints.

This is the backend for the in-app Marketplace. The registry is consumed directly by the Athanor desktop application to browse, search, and install community-built extensions.

## How It Works

1. The app fetches `registry.json` from this repo (via GitHub raw content)
2. Each entry points to a GitHub release zip on the creator's repo
3. Users install directly from the app — downloads, validates, and places files automatically
4. Verified entries have been reviewed by the Alembic team for safety

## For Creators

Want to publish a module or skill? See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

**Quick version:**
1. Build your module with a valid `module.json`
2. Create a GitHub Release with a zip asset
3. Fork this repo, add your entry to `registry.json` + `entries/<your-id>/`
4. Open a PR — our CI validates the schema automatically
5. Alembic reviews and merges

## Entry Types

| Type | Description |
|------|------------|
| `module` | Full Athanor module with UI component, `module.json` manifest |
| `skill` | One or more skill files following the `[type]module.domain.action.tN.io.ext` convention |
| `skill-bundle` | `[si]` directory bundle with `context.md` + `execute.py` + `config.json` |
| `stack-blueprint` | Exported stack topology JSON for the Forge canvas |

## Verification

- **Verified** — Reviewed by the Alembic team. Code inspected for security, tested locally.
- **Unverified** — Community-submitted, not yet reviewed. Users see a warning before installing.

## Badge System

| Badge | Criteria |
|-------|----------|
| `beta-pioneer` | Participated during beta period |
| `contributor` | Submitted at least one accepted entry |
| `bug-finder` | Reported a significant bug (manually awarded) |
| `community-specialist` | Top community engagement (manually awarded) |
| `most-used` | Download count crosses threshold (automated) |
| `verified-creator` | All submissions reviewed clean |
| `staff-pick` | Curated by the Alembic team |

## Repository Structure

```
athanor-registry/
├── registry.json                 # Master registry consumed by the app
├── schemas/
│   ├── registry.schema.json      # JSON Schema for CI validation
│   └── manifest.schema.json      # Schema for entry manifests
├── entries/<id>/                  # One directory per entry
│   ├── manifest.json
│   ├── README.md
│   └── screenshots/
├── .github/
│   ├── workflows/
│   │   ├── validate-submission.yml
│   │   └── update-download-counts.yml
│   ├── ISSUE_TEMPLATE/
│   │   └── module-submission.yml
│   └── PULL_REQUEST_TEMPLATE.md
├── CONTRIBUTING.md
└── CODE_OF_CONDUCT.md
```

## License

Registry metadata: MIT. Individual entries retain their own licenses.
