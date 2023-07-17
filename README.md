# Elastic Stacker

## Purpose

This tool is used to export configuration objects (dashboards, pipelines, alerts, etc) out of one Elastic Stack and then allow importing in to another Stack.

A main use case is for moving from development done in a PRE environment, and in to a PROD environment. Another use case is for keeping configurations in sync between multiple production stacks, but that might live on separate networks.

## Installation

```bash
pip install git+https://github.com/LLNL/elastic-stacker.git@main
```
> Publication to PyPI is on the roadmap for the future.

## Contributing

1. Clone the repository
3. Set up Python environment: `pipenv install -e .`
4. Activate the environment: `pipenv shell`
5. Concrete-ize the configuration file, specifically replacing any occurences of `<REDACTED>` with real values:
    ```sh
    cp stacker.example.yaml stacker.yaml
    # Update any configuration options needed:
    vim stacker.yaml
    ```
6. Run the tool (more detailed usage below): `stacker -p <pre|prod> system_dump`


## Usage

Running the tools

```bash
# Export all the user configurations from LC Elastic Pre
# Export should go in to the `files/` directory at the top of this repo
# this is so that the resulting exported objects can be tracked in git.
# You can also set the data directory in the config file (see the example).
./stacker.py system_dump -p pre --data-directory $(git rev-parse --show-toplevel)/files/

# Check the changes that resulted from the export, make sure things look expected
git diff

# Add and commit any changes
git add $(git rev-parse --show-toplevel)/files/
git commit -m"Updated elastic repo with exported content"
```

## License

Stacker is released under the Apache 2.0 license with LLVM exception. For more details see the LICENSE file.

LLNL-CODE-850537
