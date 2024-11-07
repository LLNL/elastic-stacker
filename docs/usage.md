# Elastic Stacker

## User manual

Elastic Stacker is a tool for importing and exporting configuration data from
Elasticsearch and Kibana instances (collectively an "*Elastic Stack*").

It can process:

- Indices
- Index templates
- Component templates
- Ingest Pipelines
- Enrich policies
- Transforms
- Watches
- Roles
- Role mappings
- Saved Objects, including but not limited to:
  - Dashboards
  - Visualizations
  - Saved queries and searches
  - Lenses
  - Index patterns

It can dump (but not load):

- Agent policies
- Package policies

These can be added to the list once the Elastic Agent APIs come out of their
alpha state and stabilize enough to work with.

It cannot currently process:

- the actual contents of indices
- ILM policies

(There is no technical reason Stacker can't handle these; it just hasn't been
done yet...if you want to contribute these features see [the contributing docs](docs/contributing.md) to get
started.)

This document will walk you through how to use Stacker to back up and restore
configuration data, or to move it between multiple stacks.

## Configuration

Stacker adjusts its behavior based on a configuration file, called
`stacker.yaml` (the `a` in yaml is optional). It looks for this file in your
current working directory, in your `$XDG_CONFIG_HOME` (this is `~/.config` on
Linux), and finally in `/etc`.

### Defaults

The top-level key in the config file is `default` -- the options in the
`default` section will be used unless overriden somewhere else. If you only need
to configure Stacker one way, it's perfectly acceptable to put all your
configuration in this section. Here's a list of configuration keys that can go
in the defaults section:

#### `client`

The client section configures the base HTTP client used for communication
with Elasticsearch and Kibana. You can set options like:

- `client.base_url`: The URL for the Elasticsearch or Kibana APIs. This should
  usually not be set in the `client` section, but instead overridden in the
  `elasticsearch` and `kibana` sections that apply to just one or the other.
- `client.timeout`: The timeout for requests, in seconds
- `client.verify`: the path to a file containing CA certificates which the
  client will trust.
- `client.auth`: a username and password to use for HTTP Basic authentication --
  set `auth.username` and `auth.password` to authenticate this way.
- `client.tls`: authentication using mutual TLS. Set `tls.cert` and `tls.key` as
  paths to the certificate and key files to use for authentication.
- `client.headers`: A map of HTTP headers to set on the request and the values
  they should take. If you want to use API keys instead of a username and
  password to interact with your Elastic Stack, you should set it as a HTTP
  header like so:
  ```yaml
  client:
    headers:
      Authorization: Apikey <key>
  ```

#### `elasticsearch` and `kibana`

These sections also configure the HTTP client -- but the settings here only
apply to either the Elasticsearch or Kibana client, respectively. This way, you
only have to set common settings (like auth headers) once in the `client`
section, but can set settings like `base_url` separately for Elasticsearch and
Kibana.

#### `substitutions`

This section can contain any number of keys, where the value is a mapping
containing the keys `search` and `replace`. For example:

```yaml
substitutions:
  distro_hop:
    search: 'Red\s?Hat( Enterprise Linux)?'
    replace: AlmaLinux
```

That substitution would find anything matching the regular expression,
including 'RedHat", "Red Hat" and "Red Hat Enterprise Linux", and replace it
with the string "AlmaLinux" in every file that Stacker writes out, and in every
dumped file it reads in as well.

This is a powerful feature, but there are some technical details to be aware of
if you want to use it.

1. These matching patterns are **regular expressions** (*regex*), a language
   for matching patterns in text. Many common punctuation characters have
   special meanings in regex, so you will need to prefix them with a backslash
   (`\`) or they may cause unexpected effects. For example, dots in domain
   names need to be escaped:

   **WRONG:** `my.domain.net`

   **RIGHT:** `my\.domain\.net`

   If you're unfamiliar with regular expressions, the [Mozilla regular expression
   cheatsheet](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_expressions/Cheatsheet)
   describes the special characters in regex and how they affect the search.

1. Stacker's config file is in the [YAML](https://yaml.org/spec/) file format,
   and YAML handles strings with backslashes in them differently depending on if
   they're `'single-quoted'` or `"double-quoted"`. In double-quoted strings, the
   YAML parser will try to interpret the backslash as the part of an escape
   sequence (for example, it'll replace `\n` with a newline character.)
   To make sure the YAML parser doesn't mangle your regex, you **MUST** put your
   `search` string in single quotes.

1. Substitutions are applied to each file in lexicographic order based on the
   name. This means that as the profile is merged with the defaults, the
   defaults may be applied before or after the profile. For example:

   ```yaml
   default:
     substitutions:
       01_one:
       03_three:
   profiles:
     staging:
       substitutions:
         00_zero:
         02_two:
   ```

   In this situation, the substitutions would be applied to your dump files in
   order, 00-03.

#### `options`

This section holds miscellaneous global options for the Stacker application.
Currently supported keys are:

- `data_directory`: Where to write out the files dumped from Elasticsearch.
- `watcher_users`: A mapping of `username: password` pairs for users in the
  Elastic [Watcher](https://www.elastic.co/guide/en/kibana/current/watcher-ui.html).
  The Elasticsearch API censors passwords from Watcher users by default, so if
  you want to load dumped watchers into another cluster you need to provide the
  password for the Watcher user separately.

#### `log`:

- `log.level`: sets the global log level for the application; accepts values
  like "INFO" or "warning"
- `log.ecs`: formats the log messages in [Elastic Common
  Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
  format for easy processing in Elasticsearch.

### Profiles

Stacker also provides a notion of *configuration profiles*. These go in the
`profiles` key at the top level of the config file. Each entry in the `profiles`
section has a name, and can contain within it all the same configuration options
as `defaults`. If you invoke `stacker` with the `--profile <name>` or `-p <name>` flags, the options you put in the named profile will override the
defaults. For example, if your configuration looks like:

```yaml
default:
  client:
    timeout: 10

profiles:
  prod:
    client:
      timeout: 30
```

In this situation, Stacker will set the client timeout to 10 seconds, unless it
is run with the `--profile prod` flag in which case it will set the timeout to
30 seconds.

It is perfectly acceptable to set options in a profile which you haven't set in
the defaults; the options will be merged together when Stacker is run.

> :information: Options given at the command line, like `--data-directory`
> override both the defaults and the profile.

## Command Line Usage

This section describes how to use Stacker's command-line flags effectively.

### Subcommands

The Stacker command requires a subcommand. You can see a list of all of them by
running `stacker` with no arguments. Most of these subcommands work the same
way: they have a `load` and a `dump` subcommand which exports or imports that
type of data into the configured data directory. For example:

```bash
stacker pipelines dump
stacker saved-objects load
```

These subcommands accept a number of flags which will be discussed more in the
next section.

There are a few other subcommands which behave differently:

- `system-dump` and `system-load`: iterate over all types of resources Stacker knows
  about, and dump or load all of them.
- `profile`: show the current configuration profile, after all the various
  levels of configuration have been merged together.
- `version`: Show the installed version of the `elastic-stacker` package.

### Flags and Parameters

In general, the individual resource subcommands (like `transforms` or
`component-templates`) accept most of the same parameters. If you want to see
what flags a particular subcommand takes, you can run `stacker --help <subcommand>` to see what flags it takes. (One exception to this is the `load`
and `dump` subcommands -- these do not respect the help flag. This is a known
bug and will be fixed in a future release.)

All commands accept the following flags:

- `--config`: The path to a Stacker config file to use.
- `--data-directory`: Where to put the dumped files, or where to look for files
  to load.
- `--profile`: Select a configuration profile from the configuration.
- `--elasticsearch`: The URL for the Elasticsearch server.
- `--kibana`: The URL for the Kibana server.
- `--ca`: the path to the CA certificate for TLS.
- `--timeout`: The timeout, in seconds, the API client should wait to receive a
  response from the server before erroring out.
- `--log-level`: the level of verbosity in the logs -- accepts the standard
  Python log level names (`ERROR`, `INFO`, `DEBUG`, etc.)
- `--ecs-log`: Log messages in Elastic Config Schema
- `--help`: Show the command's help text

All the load commands accept these flags:

- `--allow-failure`: Don't error out if a resource fails to import -- just skip
  past it.
- `--delete-after-import`: Delete files after they've been loaded into the
  target system, to make it easy to find ones that might have failed.

Most of the dump subcommands support:

- `--purge`: when dumping, delete any file that was already in the directory and
  not affected by the operation. (Usually because the resource that file referred
  to no longer exists in your stack.) In this mode, Stacker will display a list
  of files it plans to delete, and prompt you for confirmation before deleting
  them.
- `--force-purge`: The same as `--purge`, but with no prompt for confirmation
  before deleting.
- `--include-managed`: Dump the "managed" resources, which are the ones created
  by the system rather than by users. (Some resources do not have this option.)

The `system-load` subcommand accepts all the `load` flags, and also:

- `--retries`: Try this many times to load the resources into the target.
- `--temp`: Copy the dump file to a temporary directory before importing.
- `--stubborn`: Try multiple times to import things that failed, by moving the
  dump files to a temporary directory, continuing past failures, and deleting
  successfully imported resource files.

The `system-dump` subcommand accepts all the `dump` flags, and also:

- `--include-experimental`: Dump resources which cannot yet be loaded back in
  (agent policies and package policies.)

## Tips and Tricks

This section describes some recommended uses for Stacker, and how to go about
them effectively.

### Using Stacker to back up your Elastic configuration

Stacker dumps all the data it can handle as JSON files in a defined directory
structure. The easiest way to store data like this is in a Git repository.

Create a new repo, and in the root of the repo place a `stacker.yaml` file
which sets `defaults.options.data_directory: "."`. Now, while you're in this
directory, Stacker will use the configuration file in that directory first, and
dumped files will be put directly in the repo. With this setup in place, you can
back up your data by running `stacker system-dump`, then review the changes and
commit them.

### Using Stacker to move configuration between Elastic Stacks

Stacker can be used to keep configuration in sync between several different
Elastic Stacks (for example, a staging and a production environment.)

To configure Stacker for this use case, you may want to start with
[`stacker.example.yaml`](/stacker.example.yaml) which shows how this can work.

In that configuration, there's one profile for `staging` (pre-production) and one
for `production`. Both profiles share many of the same settings, but they have
different URLs, different API tokens, and potentially different passwords for
the Watcher automation user called `t1000`.

They also use the regular expression substitution to replace URLs in the dump.
When configuration is dumped from the `staging` environment, any URL that looks
like `<something>kibana<something>.cyberdyne.com` will be replaced with the
placeholder `#{KIBANA_URL}`, and the same for Elasticsearch. Because those
substitutions happen in the `default` section, they will be applied on all
profiles. But when dumped data containing those placeholders gets loaded into
the production environment, the `load_kibana_url` substitution will replace it
with the specific Kibana URL for that deployment -- but if the `staging` profile
was used instead, the staging URLs would be swapped in for that placeholder.
