# tap-hotglue

`tap-hotglue` is a Singer tap for Hotglue.


## Installation

```bash
pipx install tap-hotglue
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-hotglue --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization


## Usage

You can easily run `tap-hotglue` by itself or in a pipeline.

### Executing the Tap Directly

```bash
tap-hotglue --version
tap-hotglue --help
tap-hotglue --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_hotglue/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-hotglue` CLI interface directly using `poetry run`:

```bash
poetry run tap-hotglue --help
```
