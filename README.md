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
- **Batch starring**: Automatically star repositories across platforms
- **Starred sync with pruning**: Sync your starred repos and optionally unstar inactive ones
- **Database persistence**: SQLite and PostgreSQL support with bulk upserts
- **Streaming operations**: Efficient sync with progress reporting and batch persistence
- **Proactive rate limiting**: Client-side rate limiting to avoid API throttling
- **ETag caching**: Cache API responses with ETags to reduce rate limit usage (GitHub)
- **Graceful shutdown**: Ctrl+C handling with safe data flushing
- **Dry-run mode**: Preview operations without making changes
- **Configurable concurrency**: Control API request parallelism
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
1. Environment variables
1. Config file (`~/.config/curator/config.toml` or `./curator.toml`)
1. Built-in defaults

### Config File

```toml
[database]
url = "sqlite://~/.local/share/curator/curator.db"

[github]
token = "ghp_..."

[gitlab]
host = "gitlab.com"
token = "glpat-..."
include_subgroups = true

[codeberg]
token = "..."

[gitea]
host = "https://gitea.example.com"
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
| `CURATOR_GITLAB_TOKEN` | GitLab API token |
| `CURATOR_GITLAB_HOST` | GitLab instance host (default: gitlab.com) |
| `CURATOR_CODEBERG_TOKEN` | Codeberg API token |
| `CURATOR_GITEA_HOST` | Gitea instance URL |
| `CURATOR_GITEA_TOKEN` | Gitea API token |

The database URL defaults to `sqlite://~/.local/state/curator/curator.db` on Linux (using the XDG state directory). On macOS, it defaults to `~/Library/Application Support/curator/curator.db`.

## Usage

### Database Setup

Most CLI commands that access the database automatically apply pending migrations on startup.
Use the `migrate` command if you want explicit control or are scripting deployments.

```bash
# Run migrations
curator migrate up

# Check migration status
curator migrate status
```

### Discovery

```bash
# Crawl a site and sync discovered repositories
curator discover https://example.com

# Limit the crawl depth and pages
curator discover https://example.com --max-depth 1 --max-pages 200

# Allow external hosts and include subdomains
curator discover https://example.com --allow-external --include-subdomains
```

### GitHub

```bash
# Sync repositories from an organization
curator github org rust-lang

# Sync multiple organizations
curator github org org1 org2 org3

# Sync repositories from a user
curator github user octocat

# Preview without making changes
curator github org rust-lang --dry-run

# Filter by recent activity
curator github org rust-lang --active-within-days 30

# Sync and prune starred repositories
curator github stars

# Check rate limit status
curator github limits
```

### GitLab

```bash
# Sync projects from a group
curator gitlab group gitlab-org

# Sync from a self-hosted instance
curator gitlab group my-group --host gitlab.example.com

# Exclude subgroup projects
curator gitlab group my-group --no-subgroups

# Sync projects from a user
curator gitlab user username

# Sync starred projects
curator gitlab stars
```

### Codeberg

```bash
# Sync repositories from an organization
curator codeberg org my-org

# Sync repositories from a user
curator codeberg user username

# Sync starred repositories
curator codeberg stars
```

### Gitea

```bash
# Sync repositories from an organization
curator gitea org my-org --host https://gitea.example.com

# Sync repositories from a user
curator gitea user username --host https://gitea.example.com

# Sync starred repositories
curator gitea stars --host https://gitea.example.com
```

### Common Options

| Short | Long | Description |
| ----- | ------------------------ | -------------------------------------------- |
| `-n` | `--dry-run` | Preview without making changes |
| `-S` | `--no-star` | Skip starring repositories |
| `-a` | `--active-within-days N` | Only sync repos active within N days |
| `-c` | `--concurrency N` | Parallel API requests (default: 20) |
| `-R` | `--no-rate-limit` | Disable proactive rate limiting |
| `-H` | `--host` | Host URL (GitLab, Gitea) |
| `-s` | `--no-subgroups` | Exclude subgroups (GitLab only) |
| | `--no-prune` | Don't prune inactive starred repos (stars only) |

## Project Structure

```
crates/
├── curator/                      # Core library
│   └── src/
│       ├── lib.rs                # Library entry point
│       ├── db.rs                 # Database connection utilities
│       ├── platform.rs           # Platform trait definitions (PlatformClient)
│       ├── repository.rs         # Repository CRUD operations
│       ├── retry.rs              # Retry utilities with exponential backoff
│       ├── api_cache.rs          # ETag-based API response caching
│       ├── entity/               # SeaORM entities
│       │   ├── code_repository.rs
│       │   ├── code_platform.rs
│       │   ├── code_visibility.rs
│       │   └── api_cache.rs
│       ├── migration/            # Database migrations
│       ├── sync/                 # Unified sync engine
│       │   ├── engine.rs         # Platform-agnostic sync logic
│       │   ├── types.rs          # SyncOptions, SyncResult
│       │   └── progress.rs       # Progress callback types
│       ├── github/               # GitHub implementation
│       ├── gitlab/               # GitLab implementation
│       └── gitea/                # Gitea/Codeberg implementation
└── curator_cli/                  # Command-line interface
    └── src/
        ├── main.rs               # CLI entry point
        ├── config.rs             # Configuration loading
        └── progress.rs           # Progress bar display
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

The CLI defaults to all features enabled (`all-databases`, `github`, `gitlab`, `gitea`).

## License

See [LICENSE](LICENSE) for details.
