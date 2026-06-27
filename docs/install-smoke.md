# Install Smoke Checklist

Use this checklist on a fresh macOS aarch64 or Linux x86_64 machine before
calling Mjolnir distribution production-grade. Record the exact date, OS,
architecture, shell, installer command, release tags, and command output summary
in `PLANS.md`.

## Fresh Machine Requirements

- `bash -n install.sh` and `./install.sh --self-test` can be run locally before
  release. The self-test covers release metadata parsing, checksum URL lookup,
  and Linux/macOS asset selection without network access. It is a guardrail, not
  a replacement for the fresh-machine smoke below.
- Start from a user account that has not installed `mj`, `mjolnir`, or
  `bifrost` before.
- Use a clean install directory, for example:

```bash
export MJOLNIR_INSTALL_DIR="$HOME/.local/bin"
```

- Ensure the install directory is either already on `PATH` or accept the
  installer's shell-profile update.
- Do not reuse binaries from a development checkout.

## Shell Installer Path

Install the latest release:

```bash
curl -fsSL https://raw.githubusercontent.com/BrokkAi/mjolnir/master/install.sh | bash
```

For release-candidate validation, pin the exact tag:

```bash
MJOLNIR_VERSION=v0.10.6 \
  bash -c "$(curl -fsSL https://raw.githubusercontent.com/BrokkAi/mjolnir/master/install.sh)"
```

Expected evidence:

- Installer detects the expected OS and architecture.
- Installer downloads `mj` and `bifrost` release assets for that platform.
- Installer verifies `.sha256` checksums when published.
- `mj` and `bifrost` land in `MJOLNIR_INSTALL_DIR`.

## Commands To Verify

Run:

```bash
command -v mj
mj --version
command -v bifrost
bifrost --version
```

Then open an empty temporary git repo:

```bash
tmp="$(mktemp -d)"
cd "$tmp"
git init
mj --cwd .
```

Expected evidence:

- `mj --version` prints the installed release version.
- `mj --cwd .` opens Thor first-run setup, not an agent/model picker.
- Exiting setup with `Esc` restores the terminal cleanly.
- The platform config directory is created only if setup is saved.

## Manual Asset Path

If the shell installer fails, test the release asset manually:

1. Download the platform archive from the GitHub release page.
2. Download the adjacent `.sha256` file.
3. Verify the archive checksum.
4. Extract `mj` and run `mj --version`.

Record this as a fallback-path result, not as the primary installer result.
