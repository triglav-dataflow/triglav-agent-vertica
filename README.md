# Triglav::Agent::Vertica

Triglav Agent for Vertica

## Requirements

* Ruby >= 2.3.0

## Prerequisites

* Vertica table must have a DATE column for `daily` resource watcher
* Vertica table must have a TIMESTAMP or TIMESTAMPTZ column for `hourly` resource watcher

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'triglav-agent-vertica'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install triglav-agent-vertica

## CLI

```
Usage: triglav-agent-vertica [options]
    -c, --config VALUE               Config file (default: config.yml)
    -s, --status VALUE               Status stroage file (default: status.yml)
    -t, --token VALUE                Triglav access token storage file (default: token.yml)
        --dotenv                     Load environment variables from .env file (default: false)
    -h, --help                       help
        --log VALUE                  Log path (default: STDOUT)
        --log-level VALUE            Log level (default: info)
```

Run as:

```
bundle exec triglav-agent-vertica --dotenv -c config.yml
```

## Configuration

Prepare config.yml as:

```yaml
serverengine:
  log: 'STDOUT'
  log_level: 'debug'
  log_rotate_age: 5
  log_rotate_size: 10485760
triglav:
  url: <%= ENV['TRIGLAV_URL'] %>
  credential:
    username: <%= ENV['TRIGLAV_USERNAME'] %>
    password: <%= ENV['TRIGLAV_PASSWORD'] %>
    authenticator: local
vertica:
  watcher_interval: 60
  date_column: d
  timestamp_column: t
  connection_info:
    "vertica://":
      host: <%= ENV['VERTICA_HOST'] %>
      port: <%= ENV['VERTICA_PORT'] %>
      database: <%= ENV['VERTICA_DATABASE'] %>
      user: <%= ENV['VERTICA_USER'] %>
      password: <%= ENV['VERTICA_PASSWORD'] %>
      resource_pool: <%= ENV['VERTICA_RESOURCE_POOL'] %>
      interruptable: true
      read_timeout: 5
```

You can use erb template. You may load environment variables from .env file with `--dotenv` option as an [example](./example/example.env) file shows.

### serverengine section

You can specify any [serverengine](https://github.com/fluent/serverengine) options at this section

### triglav section

Specify triglav api url, and a credential to authenticate.

The access token obtained is stored into a token storage file (--token option).

### vertica section

This section is the special section for triglav-agent-vertica.

* **watcher_interval**: The interval to watch tables (number, default: 60)
* **connection_info**: key-value pairs of vertica connection info where keys are resource URI pattern in regular expression, and values are connection infomation

## How it behaves

1. Authenticate with triglav
  * Store the access token into the token storage file
  * Read the token from the token storage file next time
  * Refresh the access token if it is expired
2. Repeat followings in `watcher_interval` seconds:
3. Obtain resource (table) lists of the specified prefix (keys of connection_info) from triglav.
4. Connect to vertica with an appropriate connection info for a resource uri, and find tables which are newer than last check.
5. Store checking information into the status storage file for the next time check.

## Development

Prepare .env file

```
TRIGLAV_URL=http://localhost:3000
TRIGLAV_USERNAME=triglav_test
TRIGLAV_PASSWORD=triglav_test
VERTICA_HOST=xxx.xxx.xxx.xxx
VERTICA_PORT=5433
VERTICA_DATABASE=xxxx
VERTICA_USER=xxxx
VERTICA_PASSWORD=xxxx
```

Start up triglav api on localhost.

Run triglav-anget-vertica as:

```
bundle exec triglav-agent-vertica --dotenv -c example/config.yml --debug
```

The debug mode with --debug option ignores the `last_epoch` value in status file.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/triglav-workflow/triglav-agent-vertica. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

