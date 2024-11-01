# Contributing to Stacker

I'm thrilled to know you want to contribute to Stacker! This document will guide
you through the process.

## Setting up a development environment

Your first step will be to clone the repository:

```bash
git clone https://github.com/LLNL/elastic-stacker.git
```

Stacker uses [uv](https://docs.astral.sh/uv/) to manage dependencies, packaging,
etc. You can install it by following their [installation
guide](https://docs.astral.sh/uv/getting-started/installation/), and then set up
your environment by running:

```bash
# install the dependencies and development tools
uv sync

# automatically lint and format code before commiting
uv run pre-commit install

# Make a Python virtual environment to work in:
uv venv
source .venv/bin/activate
```

### The example config file

Stacker needs a config file to work. You can start with `stacker.example.yaml`
from the repository; just make a copy of it by running:

```bash
cp stacker.example.yaml stacker.yaml
```

Now you can edit the file; you'll need to change the URLs, add API tokens, etc.
(Don't worry about putting credentials in this file -- Git ignores it so it
won't get committed.)

### Code style

Stacker uses [ruff](https://docs.astral.sh/ruff/) for formatting and linting.
When you set up the environment earlier, hooks were installed into Git which
will automatically format your code, check for style issues and attempt to fix
them each time you commit. To run the same checks manually, run:

```bash
# these should be prepended with `uv run` if you don't have the venv active.
ruff check --fix
ruff format
```
