## Submission Type

- [ ] New entry
- [ ] Version update
- [ ] Metadata update (tags, description, etc.)
- [ ] Bug fix / correction

## Entry Details

- **ID:** <!-- e.g., my-module -->
- **Type:** <!-- module / skill / skill-bundle / stack-blueprint -->
- **Version:** <!-- e.g., 1.0.0 -->

## Checklist

- [ ] `id` is lowercase kebab-case, max 40 chars
- [ ] `version` is valid semver
- [ ] `core` is `false` in module.json (community submissions)
- [ ] `author` matches my GitHub username
- [ ] Repository is public and accessible
- [ ] GitHub Release exists with zip asset named `<id>-v<version>.zip`
- [ ] Zip extracts to a single directory named `<id>`
- [ ] No `.env`, `*.pem`, `*.key`, `*secret*`, or `*token*` files included
- [ ] Total extracted size under 50MB
- [ ] Entry added to `registry.json`
- [ ] `entries/<id>/` directory created with:
  - [ ] `manifest.json`
  - [ ] `README.md`
  - [ ] At least one screenshot in `screenshots/`
- [ ] `LICENSE` file present in my source repository

## Description

<!-- Brief description of what this entry does and why it's useful -->

## Screenshots

<!-- Paste or link at least one screenshot showing your module/skill in action -->

## Testing

<!-- How did you test this? What Athanor version did you test against? -->
