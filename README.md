# carwatch

Monitor finn.no classifieds for changes. Crawls search result pages, stores per-ad snapshots in a local SQLite database, detects field-level changes over time, and presents results in an interactive terminal UI.

## Changelog

### 0.1.1 — 2026-03-24

- fix: replace heredoc in release workflow with grouped `echo` block to avoid YAML block-scalar parse error

### 0.1.0

- Initial release