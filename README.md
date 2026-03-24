# finnwatch

Monitor finn.no classifieds for changes. Crawls search result pages, stores per-ad snapshots in a local SQLite database, detects field-level changes over time, and presents results in an interactive terminal UI.

## Changelog

### 0.1.5 — 2026-03-24

- fix(pkg): install the real `fw-tui` bash script into the deb package instead
  of replacing it with a minimal Python one-liner; create the per-user venv with
  `--system-site-packages` so `finnwatch_tui`/`finnwatch_core` from
  `/usr/lib/python3/dist-packages/` are visible; add `bash` to deb `Depends:`

### 0.1.4 — 2026-03-24

- refactor: rename project from `carwatch` to `finnwatch` across pyproject.toml, README, release workflow, and egg-info

### 0.1.3 — 2026-03-24

- fix(install): remove stale `[console_scripts]` entry from egg-info that caused
  the deb build to install `fw-tui` as a plain Python wrapper instead of the
  bash venv-bootstrap script; add `bin/fw-tui` to `SOURCES.txt`

### 0.1.2 — 2026-03-24

- fix(install): install `fw-tui` as a bash script-file instead of a Python entry
  point so the venv-bootstrap logic runs when installed from the deb package;
  detect system-install vs source-tree at runtime and store the per-user venv
  under `~/.local/share/finnwatch/venv` when running from a system prefix

### 0.1.1 — 2026-03-24

- fix: replace heredoc in release workflow with grouped `echo` block to avoid YAML block-scalar parse error

### 0.1.0

- Initial release