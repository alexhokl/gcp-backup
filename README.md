## Installation

```sh
go install github.com/alexhokl/gcp-backup@latest
```

## Configuration

Configuration should be stored in path `$HOME/.gcp-backup.yml`.

The following is an example.

```yaml
bucket: machine-backup
machine_alias: machines/mac14
path_to_application_default_credentials: /Users/someone/.config/gcloud/legacy_credentials/someone@test.com/adc.json
paths:
  - .config/asciinema/
  - .config/atuin/
  - .aws/
  - .kube/config
  - .ssh/
```

## Usage

### Backup (dry-run)

```sh
gcp-backup run --dry-run
```

### Backup

```sh
gcp-backup run
```

