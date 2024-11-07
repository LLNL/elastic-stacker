# Contributing to Stacker

I'm thrilled to know you want to contribute to Stacker! This document will guide
you through the process.

## Setting up a development environment

Your first step will be to clone the repository:

```bash
git clone https://github.com/LLNL/elastic-stacker.git
```

Stacker uses [Hatch](https://hatch.pypa.io/latest/) to manage dependencies,
packaging, etc. You can install it by following their [installation
guide](https://docs.astral.sh/uv/getting-started/installation/), and then set up
your environment by running:

```bash
# automatically lint and format code before commiting
hatch run pre-commit install

# Enter the project's virtual environment:
hatch shell
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

Code style is enforced by `hatch fmt`, a wrapper around `ruff` with most of the
extended checks turned on. You can run it like this:

```bash
hatch fmt
```

There's also a pre-commit hook that runs ruff's basic linting and formatting
before each commit.
