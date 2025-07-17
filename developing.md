# Developing

## Development Environment Setup

### With direnv (Recommended)

If you have [direnv](https://direnv.net/) installed, environment setup is automatic:

```bash
# One-time setup: allow direnv to load .envrc
$ direnv allow

# Environment will be automatically activated when entering the directory
# Dependencies will be synced automatically
```

### Manual Setup with uv

```bash
# Install dependencies (includes dev dependencies by default)
$ uv sync --extra-index-url https://apsis-scheduler.github.io/procstar/simple

# Or include documentation dependencies too
$ uv sync --extra-index-url https://apsis-scheduler.github.io/procstar/simple --group docs

# Activate the virtual environment
$ source .venv/bin/activate
```

To initialize an instance DB,
```
$ apsisctl create apsis.db
```

Create a config file, for example:
```yaml
# Path to the state database.
database: apsis.db

# Path to the jobs directory.
job_dirs: jobs

# Lookback in secs for old runs.
# Currently, applied when loading runs from database at startup.
runs_lookback: 2592000  # 30 days

# Refuse to schedule runs older than this.
schedule_max_age: 86400  # 1 day
```

To run the back end,
```
$ apsisctl serve -c config.yaml
```


### Web UI

This repo contains a copy of the prod front end, which you can use if
you are not developing the front end itself.

The Vue front end was set up with the [webpack vuejs
template](https://vuejs-templates.github.io/webpack/) (an older version).

#### Developing

Currently requires node 14 (`nvm use 14`), until we update a bunch of stuff.

To run the front end in dev mode,
```
$ cd vue
$ npm install
$ npm run dev
```

To run js unit tests:
```
$ npm run unit
```

Front end e2e tests not set up.


#### Packaging

To produce a prod front end, 
```
$ cd vue
$ npm run build
```
then
```
$ git add --all .
$ git commit -m "Rebuild front end."
```

The Python back end service will serve the prod front end.


## Pre-commit Setup

This project uses pre-commit hooks to automatically format code before commits.

### Installation
Install the pre-commit hooks:
```bash
$ uv run pre-commit install
```

### Usage

Once installed, the pre-commit hooks will run automatically on `git commit`. The hooks will:
- Format Python code using `ruff format`
- Only run on files that are being committed

To run the hooks manually on all files:
```bash
$ pre-commit run --all-files
```

To run the hooks on specific files:
```bash
$ pre-commit run --files path/to/file.py
```

### Bypassing Hooks

If you need to bypass the pre-commit hooks (not recommended):
```bash
$ git commit --no-verify -m "commit message"
```


### Docs

With uv, install the docs dependency group:
```bash
$ uv sync --group docs
$ cd docs
$ make html
```



# Tests

Run Python tests (dev dependencies are included by default with `uv sync`):
```bash
$ pytest
```

