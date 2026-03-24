# carwatch

Monitor finn.no classifieds for changes. Crawls search result pages, stores per-ad snapshots in a local SQLite database, detects field-level changes over time, and presents results in an interactive terminal UI.

## Changelog

### 0.1.2 — 2026-03-24

- fix(install): install `fw-tui` as a bash script-file instead of a Python entry
  point so the venv-bootstrap logic runs when installed from the deb package;
  detect system-install vs source-tree at runtime and store the per-user venv
  under `~/.local/share/finnwatch/venv` when running from a system prefix

### 0.1.1 — 2026-03-24

- fix: replace heredoc in release workflow with grouped `echo` block to avoid YAML block-scalar parse error

### 0.1.0

- Initial release