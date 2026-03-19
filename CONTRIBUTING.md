# Contributing to the Athanor Registry

Thank you for building on Open Athanor. This guide covers everything you need to publish a **module**, **skill**, or **stack blueprint** to the community marketplace.

---

## Table of Contents

1. [How the Marketplace Works](#how-the-marketplace-works)
2. [Entry Types](#entry-types)
3. [Prerequisites](#prerequisites)
4. [Author Identity](#author-identity)
5. [Preparing Your Entry](#preparing-your-entry)
6. [Submitting to the Registry](#submitting-to-the-registry)
7. [Review Process](#review-process)
8. [Updating Your Entry](#updating-your-entry)
9. [Schema Reference](#schema-reference)
10. [Code of Conduct](#code-of-conduct)
11. [Beta Incentives](#beta-incentives)

---

## How the Marketplace Works

The Athanor marketplace is powered by this GitHub repository. There is no separate backend — the app fetches `registry.json` directly from this repo's `main` branch.

**Flow:**
1. You prepare your module/skill/stack and host the source in a GitHub repo
2. You fork this registry, add your entry files, and open a Pull Request
3. After review and merge, your entry appears in every Athanor user's Marketplace tab
4. When a user clicks **Install**, Athanor clones your entry directory from `entries/<your-id>/` directly into `~/.athanor/modules/` (or `skills/` or `stacks/`)

**No uploads happen inside the Athanor app.** All publishing goes through GitHub.

---

## Entry Types

| Type | Description | Install Path |
|------|-------------|--------------|
| `module` | Full plug-and-play Athanor module with `module.json` | `~/.athanor/modules/<id>/` |
| `skill` | A single skill file (inference prompt, script, or bundle) | `~/.athanor/skills/<id>/` |
| `skill-bundle` | A directory of related skills packaged together | `~/.athanor/skills/<id>/` |
| `stack-blueprint` | A reusable stack configuration template | `~/.athanor/stacks/<id>/` |

---

## Prerequisites

- A **GitHub account**
- A **public GitHub repository** containing your module/skill/stack source code
- **Open Athanor** installed locally to test your entry before submitting

---

## Author Identity

You have two options for attribution:

### Option A: GitHub Username Only (Anonymous)
Simply use your GitHub username in the `author` field. No registration required. Your entry will be tagged to your GitHub account.

### Option B: Verified Creator (Recommended)
To earn the `verified-creator` badge and unlock future features:

1. Open an issue in this repo titled **"Creator Verification: @yourusername"**
2. Include:
   - Your GitHub username
   - A brief description of what you plan to publish
   - Contact email (optional — for security notifications only)
3. The Alembic team will verify your identity and add the `verified-creator` author badge

Verified creators get:
- A checkmark next to their name in the marketplace
- Priority review on submissions
- Access to future creator features (analytics, featured placement, etc.)

> **Note:** Full in-app account registration is coming in a future release. For now, all identity is tied to your GitHub account.

---

## Preparing Your Entry

### Modules

A module is the primary building block. It integrates directly into Athanor's module system and can include its own tab, skills, and capabilities.

**Required structure:**
```
your-module/
├── module.json           # Required — Athanor module manifest
├── README.md             # Required — shown in the marketplace detail view
├── manifest.json         # Required — registry metadata (see schema below)
├── LICENSE               # Required
├── screenshots/          # Recommended (at least 1 for verification)
│   └── screenshot-1.png
└── your_source/          # Your module's source code
    ├── __init__.py       # (or index.js, etc.)
    └── ...
```

**module.json** — This is the standard Athanor module manifest that tells the app how to load your module:

```json
{
  "id": "your_module",
  "name": "Your Module",
  "version": "1.0.0",
  "description": "What your module does",
  "author": "your-github-username",
  "license": "MIT",
  "icon": "Y",
  "core": false,
  "enabled": true,
  "tab": {
    "label": "Your Module",
    "component": "your_module"
  },
  "capabilities": ["capability-one", "capability-two"],
  "interaction": "interactive"
}
```

**Key rules:**
- `id` must be lowercase with underscores (this is the internal module ID)
- `core` must be `false` — only Athanor built-ins are core
- `interaction` must be one of: `interactive`, `output`, `observe`, `background`
- The `tab.component` value must match your module's internal component name

### Skills

A skill is a single unit of intelligence — an inference prompt, a script, or a bundle.

**Required structure:**
```
your-skill/
├── manifest.json         # Required — registry metadata
├── README.md             # Required
└── [type]module.domain.action.tN.io.ext
```

**Skill filename convention:**
- `[i]` prefix = inference/instructive skill (`.md`)
- `[s]` prefix = script skill (`.py`, `.sh`, `.js`)
- `[si]` prefix = bundle (directory)
- Format: `[type]module.domain.action.tN.io.ext`
  - `module` = which module this skill belongs to
  - `domain` = knowledge domain
  - `action` = what the skill does
  - `tN` = tier (t1 = basic, t2 = intermediate, t3 = advanced)
  - `io` = input/output format (e.g., `t2s` = text to structured)

**Example:** `[i]analysis.finance.sentiment.t2.t2s.md`

### Stack Blueprints

A stack blueprint is a reusable configuration template for Athanor's stack system.

**Required structure:**
```
your-stack/
├── manifest.json         # Required — registry metadata
├── README.md             # Required
└── stack.json            # Required — the stack configuration
```

---

## Submitting to the Registry

### Step 1: Fork This Repo

Fork `alembic-ai/athanor-registry` to your GitHub account.

### Step 2: Add Your Entry Directory

Create `entries/<your-id>/` with all your files:

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/athanor-registry.git
cd athanor-registry

# Create your entry directory
mkdir -p entries/your-module

# Copy your module files
cp -r /path/to/your-module/* entries/your-module/
```

**Important:** The entire `entries/<your-id>/` directory is what Athanor clones when a user installs. Include everything needed for the module to work — source code, module.json, README, etc.

### Step 3: Create manifest.json

Inside `entries/<your-id>/manifest.json`:

```json
{
  "id": "your-module",
  "name": "Your Module",
  "type": "module",
  "author": "your-github-username",
  "repo": "https://github.com/you/your-module",
  "version": "1.0.0",
  "min_athanor_version": "0.1.0",
  "description": "Short description of your module (max 300 chars)",
  "license": "MIT",
  "tags": ["tag1", "tag2"],
  "capabilities": ["cap-one", "cap-two"],
  "interaction_type": "interactive",
  "use_case_tags": ["developer"],
  "swarm_size_tags": ["all"]
}
```

### Step 4: Add to registry.json

Add your entry to the `entries` array in `registry.json`:

```json
{
  "id": "your-module",
  "name": "Your Module",
  "type": "module",
  "author": "your-github-username",
  "repo": "https://github.com/you/your-module",
  "release_zip_url": "",
  "version": "1.0.0",
  "min_athanor_version": "0.1.0",
  "verified": false,
  "featured": false,
  "tags": ["tag1", "tag2"],
  "description": "Short description (max 300 chars)",
  "icon_emoji": null,
  "download_count": 0,
  "badges": [],
  "author_badges": [],
  "license": "MIT",
  "interaction_type": "interactive",
  "capabilities": ["cap-one"],
  "use_case_tags": ["developer"],
  "swarm_size_tags": ["all"],
  "created_at": "2026-03-18T00:00:00Z",
  "updated_at": "2026-03-18T00:00:00Z",
  "bundled": false,
  "paywalled": false
}
```

**Important fields:**
- `id` — must be lowercase kebab-case, match `^[a-z][a-z0-9\-_]*$`, max 40 chars
- `verified` — always set to `false` (the Alembic team sets this after review)
- `featured` — always set to `false` (staff-curated only)
- `bundled` — always `false` for community submissions
- `release_zip_url` — optional. If empty, Athanor clones directly from `entries/<id>/`. If you prefer distributing via GitHub Releases, set this to the direct download URL of your zip asset.

### Step 5: Open a Pull Request

Push to your fork and open a PR against `main`. Include:
- A summary of what your entry does
- How to test it
- Screenshots if applicable

**From the Athanor app:** Click the **"Submit via GitHub"** button in the Marketplace tab — this opens the registry repo directly.

---

## Review Process

After you open a PR:

1. **Automated validation** checks:
   - JSON schema compliance against `schemas/manifest.schema.json` and `schemas/registry.schema.json`
   - Entry ID matches directory name
   - No duplicate IDs
   - No prohibited files (`.env`, `*.key`, `*.pem`, credentials)
   - Total size under 50MB

2. **Manual review** by the Alembic team:
   - Code review for security (no malicious code, backdoors, data exfiltration)
   - Functionality testing in a local Athanor instance
   - Quality check (does it work? is the README clear?)

3. **Outcome:**
   - Merged as `verified: true` — full endorsement
   - Merged as `verified: false` — works but not yet fully reviewed
   - Changes requested — feedback provided
   - Rejected — reason provided

**Timeline:** We aim to review PRs within 48 hours during beta.

---

## Updating Your Entry

To publish a new version:

1. Update the files in `entries/<your-id>/`
2. Update `version` and `updated_at` in both `manifest.json` and `registry.json`
3. If you use release zips, update `release_zip_url` to the new release
4. Open a PR

---

## Schema Reference

Full JSON schemas are available in `schemas/`:
- [`registry.schema.json`](schemas/registry.schema.json) — validates the top-level `registry.json`
- [`manifest.schema.json`](schemas/manifest.schema.json) — validates individual `entries/<id>/manifest.json`

### registry.json Entry Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique lowercase kebab-case identifier |
| `name` | string | Yes | Human-readable display name |
| `type` | enum | Yes | `module`, `skill`, `skill-bundle`, or `stack-blueprint` |
| `author` | string | Yes | GitHub username |
| `repo` | string | Yes | GitHub repository URL |
| `release_zip_url` | string | No | Direct URL to release zip (empty = clone from entries/) |
| `version` | string | Yes | Semver version |
| `min_athanor_version` | string | Yes | Minimum compatible Athanor version |
| `verified` | boolean | Yes | Set by Alembic team (submit as `false`) |
| `featured` | boolean | Yes | Set by Alembic team (submit as `false`) |
| `tags` | string[] | Yes | Searchable tags |
| `description` | string | Yes | Short description (max 300 chars) |
| `icon_emoji` | string or null | No | Optional emoji icon |
| `download_count` | integer | Yes | Total installs (submit as `0`) |
| `badges` | string[] | Yes | Entry badges (submit as `[]`) |
| `author_badges` | string[] | Yes | Author badges (submit as `[]`) |
| `license` | string | Yes | SPDX license identifier |
| `interaction_type` | enum | Yes | `interactive`, `output`, `observe`, or `background` |
| `capabilities` | string[] | Yes | Machine-readable capability tags |
| `use_case_tags` | enum[] | Yes | `developer`, `researcher`, `trader`, `creative`, `operations`, `general` |
| `swarm_size_tags` | enum[] | Yes | `solo`, `small`, `large`, `all` |
| `created_at` | string | Yes | ISO 8601 timestamp |
| `updated_at` | string | Yes | ISO 8601 timestamp |
| `bundled` | boolean | Yes | Always `false` for community entries |
| `paywalled` | boolean | Yes | `false` (future use) |

### Available Badges

**Entry badges** (set by Alembic team):
- `staff-pick` — Curated by the team
- `most-used` — High download count

**Author badges** (earned):
- `verified-creator` — Identity verified
- `beta-pioneer` — Submitted during beta
- `contributor` — Accepted submission
- `bug-finder` — Reported bugs
- `community-specialist` — Helped others, reviewed code

---

## Code of Conduct

By submitting, you agree to:
- Only publish code you own or have rights to distribute
- Not include malicious code, backdoors, or data exfiltration
- Respect the Athanor ecosystem and other creators
- Accept that Alembic may remove entries that violate these terms

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for the full policy.

---

## Beta Incentives

During beta, active contributors earn badges displayed in the marketplace:

| Badge | How to Earn |
|-------|-------------|
| **beta-pioneer** | First accepted submission during beta |
| **contributor** | Each accepted submission |
| **bug-finder** | Report bugs in modules or the marketplace |
| **most-used** | Your entry hits download thresholds |
| **community-specialist** | Help others, review code, write guides |

Top contributors will be recognized and rewarded.

---

## Questions?

- Open an [issue](https://github.com/alembic-ai/athanor-registry/issues) in this repo
- Review existing entries in `entries/` for reference (e.g., `entries/omni-data/`)
- Check the [Athanor documentation](https://github.com/alembic-ai/opn_athnr_beta)
