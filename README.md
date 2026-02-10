# Curator

[![CI](https://github.com/gkze/curator/actions/workflows/ci.yml/badge.svg)](https://github.com/gkze/curator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.92.0-orange.svg)](https://www.rust-lang.org/)

A multi-platform repository tracker and manager that provides a unified interface for discovering, tracking, and managing repositories across multiple code hosting platforms.

## Supported Platforms

- **GitHub** - github.com
- **GitLab** - gitlab.com and self-hosted instances
- **Codeberg** - codeberg.org
- **Gitea/Forgejo** - self-hosted instances

## Features

- **Unified sync**: Fetch and store repository metadata from organizations/groups/users into a local database
- **Activity filtering**: Filter repositories by last activity date (configurable, default 60 days)
- **Incremental sync**: Skip repositories that haven't changed since the last sync
- **Batch starring**: Automatically star repositories across platforms
- **Starred sync with pruning**: Sync your starred repos and optionally unstar inactive ones
- **Database persistence**: SQLite and PostgreSQL support with bulk upserts
- **Streaming operations**: Efficient sync with progress reporting and batch persistence
- **Proactive rate limiting**: Client-side rate limiting to avoid API throttling
- **ETag caching**: Cache API responses with ETags to reduce rate limit usage (GitHub, GitLab)
- **Graceful shutdown**: Ctrl+C handling with safe data flushing
- **Dry-run mode**: Preview operations without making changes
- **Configurable concurrency**: Control API request parallelism
- **Instance management**: Add/list/remove platform instances in the local database
- **OAuth login**: Device/PKCE flows for GitHub, GitLab, and Codeberg
- **Shell completions**: Generate completions for bash, zsh, fish, etc.
- **Man pages**: Generate man pages for offline documentation

## Installation

### From GitHub Releases

Download pre-built binaries from the [GitHub Releases](https://github.com/gkze/curator/releases) page:

```bash
# macOS (Apple Silicon)
curl -L https://github.com/gkze/curator/releases/latest/download/curator-aarch64-apple-darwin.tar.gz | tar xz

# Linux (x86_64)
curl -L https://github.com/gkze/curator/releases/latest/download/curator-x86_64-unknown-linux-gnu.tar.gz | tar xz

# Move to PATH
sudo mv curator /usr/local/bin/
```

### From Source (Cargo)

```bash
# Clone and install locally
git clone https://github.com/gkze/curator.git
cd curator
cargo install --path crates/curator_cli
```

### With Nix

```bash
nix profile add github:gkze/curator
```

## Configuration

Curator uses a layered configuration system. Settings are resolved in this order (highest priority first):

1. CLI flags
1. Environment variables (including `.env` in the current directory)
1. Config file (`~/.config/curator/config.toml` or `./curator.toml`)
1. Built-in defaults

### Config File

```toml
[database]
url = "sqlite://~/.local/state/curator/curator.db?mode=rwc"

[github]
token = "ghp_..."

[gitlab]
token = "glpat-..."
refresh_token = "..."      # optional; used for OAuth auto-refresh
token_expires_at = 1735689600 # optional unix timestamp

[codeberg]
token = "..."
refresh_token = "..."      # optional; used for OAuth auto-refresh
token_expires_at = 1735689600 # optional unix timestamp

[gitea]
host = "gitea.example.com"
token = "..."

[sync]
active_within_days = 60
concurrency = 20
star = true
no_rate_limit = false
```

### Environment Variables

All environment variables use the `CURATOR_` prefix:

| Variable | Description |
| ------------------------ | --------------------------------------------------- |
| `CURATOR_DATABASE_URL` | Database connection string (default: see below) |
| `CURATOR_GITHUB_TOKEN` | GitHub API token |
| `CURATOR_GITLAB_HOST` | GitLab host (default: `gitlab.com`) |
| `CURATOR_GITLAB_TOKEN` | GitLab API token |
| `CURATOR_GITLAB_REFRESH_TOKEN` | GitLab OAuth refresh token |
| `CURATOR_GITLAB_TOKEN_EXPIRES_AT` | GitLab OAuth token expiry (unix seconds) |
| `CURATOR_CODEBERG_TOKEN` | Codeberg API token |
| `CURATOR_CODEBERG_REFRESH_TOKEN` | Codeberg OAuth refresh token |
| `CURATOR_CODEBERG_TOKEN_EXPIRES_AT` | Codeberg OAuth token expiry (unix seconds) |
| `CURATOR_GITEA_HOST` | Gitea/Forgejo host |
| `CURATOR_GITEA_TOKEN` | Gitea API token |

The database URL defaults to `sqlite://~/.local/state/curator/curator.db?mode=rwc` on Linux (using the XDG state directory). On macOS, it defaults to `sqlite://~/Library/Application Support/curator/curator.db?mode=rwc`.

## Usage

### Instances & Auth

Curator syncs against instances stored in the local database. Add an instance first, then authenticate.

```bash
# Add well-known instances
curator instance add github
curator instance add gitlab
curator instance add codeberg

# Add a self-hosted instance
curator instance add work-gitlab -t gitlab -H gitlab.mycompany.com
curator instance add my-gitea -t gitea -H gitea.example.com

# List configured instances
curator instance list

# Update auth settings for an instance
curator instance update work-gitlab -c my-oauth-client-id -f device

# OAuth login (GitHub/GitLab/Codeberg)
curator login github
curator login gitlab
curator login codeberg
```

OAuth login is supported for well-known instances (github.com, gitlab.com, codeberg.org).
For GitHub Enterprise or self-hosted GitLab/Gitea, configure a PAT via `CURATOR_*_TOKEN`.

### Database Setup

Most CLI commands that access the database automatically apply pending migrations on startup.
Use the `migrate` command if you want explicit control or are scripting deployments.

```bash
# Run migrations
curator migrate up

# Check migration status
curator migrate status

# Roll back the last migration
curator migrate down

# Drop all tables and reapply migrations
curator migrate fresh
```

### Discovery

```bash
# Crawl a site and sync discovered repositories
curator discover https://example.com

# Limit the crawl depth and pages
curator discover https://example.com --max-depth 1 --max-pages 200

# Allow external hosts and include subdomains
curator discover https://example.com --allow-external --include-subdomains

# Use sitemap discovery and tune crawl concurrency
curator discover https://example.com --crawl-concurrency 20

# Disable sitemap discovery
curator discover https://example.com --no-sitemaps
```

Discovery only syncs repositories for hosts that have configured instances.
Add those instances first (e.g., `curator instance add github`).

### GitHub

```bash
# Sync repositories from an organization
curator sync org github rust-lang

# Sync multiple organizations
curator sync org github org1 org2 org3

# Sync repositories from a user
curator sync user github octocat

# Preview without making changes
curator sync org github rust-lang --dry-run

# Filter by recent activity
curator sync org github rust-lang --active-within-days 30

# Sync and prune starred repositories
curator sync stars github

# Check rate limit status
curator limits github
```

### GitLab

```bash
# Sync projects from a group
curator sync org gitlab gitlab-org

# Sync from a self-hosted instance
curator sync org work-gitlab my-group

# Exclude subgroup projects
curator sync org gitlab my-group --no-subgroups

# Sync projects from a user
curator sync user gitlab username

# Sync starred projects
curator sync stars gitlab
```

### Codeberg

```bash
# Sync repositories from an organization
curator sync org codeberg my-org

# Sync repositories from a user
curator sync user codeberg username

# Sync starred repositories
curator sync stars codeberg
```

### Gitea

```bash
# Sync repositories from an organization
curator sync org my-gitea my-org

# Sync repositories from a user
curator sync user my-gitea username

# Sync starred repositories
curator sync stars my-gitea
```

### Common Options

| Short | Long | Description |
| ----- | ------------------------ | -------------------------------------------- |
| `-n` | `--dry-run` | Preview without making changes |
| `-S` | `--no-star` | Skip starring repositories |
| `-a` | `--active-within-days N` | Only sync repos active within N days |
| `-c` | `--concurrency N` | Parallel API requests (default: 20) |
| `-R` | `--no-rate-limit` | Disable proactive rate limiting |
| `-s` | `--no-subgroups` | Exclude subgroups (GitLab only) |
| `-i` | `--incremental` | Incremental sync (org/user only) |
| `-P` | `--no-prune` | Don't prune inactive starred repos (stars only) |

## Project Structure

```
crates/
├── curator/                      # Core library
│   └── src/
│       ├── lib.rs                # Library entry point
│       ├── db.rs                 # Database connection utilities
│       ├── instance.rs           # Instance helpers
│       ├── platform.rs           # Platform trait definitions (PlatformClient)
│       ├── platform/             # Platform helpers (rate limits, conversion)
│       ├── repository.rs         # Repository API surface
│       ├── repository/           # Repository CRUD and queries
│       ├── sync/                 # Unified sync engine
│       ├── discovery/            # Discovery crawler (feature)
│       ├── oauth/                # OAuth helpers
│       ├── api_cache.rs          # ETag-based API response caching
│       ├── entity/               # SeaORM entities
│       │   ├── instance.rs
│       │   ├── code_repository.rs
│       │   ├── code_visibility.rs
│       │   ├── platform_type.rs
│       │   ├── platform_metadata.rs
│       │   └── api_cache.rs
│       ├── migration/            # Database migrations
│       ├── github/               # GitHub implementation
│       ├── gitlab/               # GitLab implementation
│       └── gitea/                # Gitea/Codeberg implementation
└── curator_cli/                  # Command-line interface
    └── src/
        ├── main.rs               # CLI entry point
        ├── config.rs             # Configuration loading
        ├── commands/             # CLI subcommands
        ├── progress.rs           # Progress bar display
        └── shutdown.rs           # Ctrl+C handling
```

## Building

```bash
# Build with all features
cargo build --release

# Build with specific features
cargo build --release --features "sqlite,github,gitlab"

# Run tests
cargo test
```

### Feature Flags

**Database backends:**

- `sqlite` - SQLite database support
- `postgres` - PostgreSQL database support
- `all-databases` - Both SQLite and PostgreSQL support
- `migrate` - Database migration support (combine with database backend)

**Platform clients:**

- `github` - GitHub platform support
- `gitlab` - GitLab platform support
- `gitea` - Gitea/Codeberg platform support
- `discovery` - Website crawling and repo link discovery

The CLI defaults to all features enabled (`all-databases`, `github`, `gitlab`, `gitea`, `discovery`).

## License

See [LICENSE](LICENSE) for details.
