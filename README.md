# Elastic Stacker

## Purpose

This tool is used to export configuration objects (dashboards, pipelines,
alerts, etc) out of one Elastic Stack and then allow importing in to another
Stack.

A main use case is for moving from development done in a staging environment,
and in to a production environment. Another use case is to keep configurations
in sync between multiple production stacks, but that might live on separate
networks.

## Installation

Stacker is on PyPI; you can install it by running:

```bash
pip install elastic-stacker
```

## Usage

Some simple invocations of Stacker:

Dump all ingest pipelines as JSON files, using all the default settings:

```bash
stacker pipelines dump
```

Dump all role mappings, deleting any files for mappings that no longer exist:

```bash
 stacker role-mappings dump --purge
```

Load the ingest pipelines into Elasticsearch, using the "prod" config profile:

```bash
stacker pipelines load --profile prod
```

Dump all resources Stacker can handle into a different data directory:

```bash
stacker system-dump --data-directory=../elastic-data
# (you can load this back in with the "system-load" subcommand)
```

specify the URLs of the Elasticsearch or Kibana APIs:

```bash
stacker --elasticsearch <URL> --kibana <URL> <SUBCOMMAND>
```

For more detailed instructions, see [usage.md](docs/usage.md). Many
more configuration options can be set in the config file -- see
[usage.md](docs/usage.md#Configuration) for a full list.

## Contributing

You want to hack on Stacker? Awesome, your contributions are welcome. See
[contributing.md](docs/contributing.md) for details.

## License

Stacker is released under the Apache 2.0 license with LLVM exception. For
more details see the [LICENSE](LICENSE) file.

LLNL-CODE-850537
