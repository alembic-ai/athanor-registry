# Contributing to the Athanor Registry

Thank you for building on Open Athanor. This guide covers everything you need to publish a module, skill, or stack blueprint to the marketplace.

## Prerequisites

- A GitHub account
- A public GitHub repository containing your module/skill
- Familiarity with the [module.json manifest format](https://github.com/alembic-ai/opn_athnr_beta)

## Step 1: Prepare Your Repository

Your repo must contain at minimum:

```
your-module/
├── module.json           # Required — standard Athanor manifest
├── README.md             # Required — shown in marketplace detail view
├── LICENSE               # Required — SPDX identifier
├── skills/               # Optional — skill files
│   └── [i]yourmod.*.md
└── screenshots/          # Required for verification (min 1)
    └── screenshot-1.png
```

### module.json Requirements

All standard `module.json` fields plus an optional `marketplace` block:

```json
{
  "id": "your-module",
  "name": "Your Module",
  "version": "1.0.0",
  "description": "What your module does (max 300 chars)",
  "author": "your-github-username",
  "license": "MIT",
  "icon": "Y",
  "core": false,
  "enabled": true,
  "interaction": "interactive",
  "tab": {
    "label": "Your Module",
    "component": "your-module"
  },
  "capabilities": ["capability-one", "capability-two"],
  "marketplace": {
    "registry_id": "your-module",
    "min_athanor_version": "0.1.0",
    "homepage": "https://github.com/you/your-module",
    "keywords": ["tag1", "tag2"]
  }
}
```

**Validation rules:**
- `id` must be lowercase, kebab-case, max 40 chars, match `^[a-z][a-z0-9-_]*$`
- `version` must be valid semver
- `core` must be `false` for community submissions
- `author` must not be empty
- No `.env`, `*.pem`, `*.key`, `*secret*`, `*token*` files
- Total extracted size under 50MB

## Step 2: Create a GitHub Release

1. Tag your repo with a semver tag (e.g., `v1.0.0`)
2. Create a GitHub Release from that tag
3. Attach a zip asset named `<id>-v<version>.zip`
4. The zip must extract to a single directory named `<id>`

Example:
```
your-module-v1.0.0.zip
└── your-module/
    ├── module.json
    ├── README.md
    ├── LICENSE
    └── ...
```

## Step 3: Submit a PR

1. Fork `alembic-ai/athanor-registry`
2. Add your entry to `registry.json`:

```json
{
  "id": "your-module",
  "name": "Your Module",
  "type": "module",
  "author": "your-github-username",
  "repo": "https://github.com/you/your-module",
  "release_zip_url": "https://github.com/you/your-module/releases/download/v1.0.0/your-module-v1.0.0.zip",
  "version": "1.0.0",
  "min_athanor_version": "0.1.0",
  "verified": false,
  "featured": false,
  "tags": ["tag1", "tag2"],
  "description": "What your module does.",
  "icon_emoji": null,
  "download_count": 0,
  "badges": [],
  "author_badges": [],
  "license": "MIT",
  "interaction_type": "interactive",
  "capabilities": ["cap-one"],
  "use_case_tags": ["developer"],
  "swarm_size_tags": ["all"],
  "created_at": "2026-03-15T00:00:00Z",
  "updated_at": "2026-03-15T00:00:00Z",
  "bundled": false,
  "paywalled": false
}
```

3. Create `entries/<your-id>/`:
   - `manifest.json` — copy of your registry entry (can have extended fields)
   - `README.md` — copy of your repo's README
   - `screenshots/` — at least one screenshot

4. Open a PR against `main`

## Step 4: Review

Our CI validates:
- JSON schema compliance
- ID matches directory name
- Repo URL is reachable
- No duplicate IDs

The Alembic team then:
1. Reviews code for security
2. Tests the module locally
3. Sets `verified: true` (or merges as unverified)
4. Optionally adds `featured: true` and `staff-pick` badge

## After Merge

Your entry appears in the marketplace within 1 hour (cache refresh).

## Updating Your Entry

To publish a new version:
1. Create a new GitHub Release with the updated zip
2. Open a PR updating `version`, `release_zip_url`, and `updated_at` in `registry.json`

## Code of Conduct

By submitting, you agree to:
- Only publish code you own or have rights to distribute
- Not include malicious code, backdoors, or data exfiltration
- Respect the Athanor ecosystem and other creators
- Accept that Alembic may remove entries that violate these terms

## Beta Incentives

During beta, active contributors earn badges:
- **beta-pioneer** — Auto-awarded on first accepted submission
- **contributor** — Awarded for each accepted submission
- **bug-finder** — Report bugs in modules or the marketplace itself
- **most-used** — Your module hits download thresholds
- **community-specialist** — Help others, review code, write guides

Top contributors will be recognized and rewarded. Stay tuned for announcements.

## Questions?

Open an issue in this repo or reach out on our community channels.
