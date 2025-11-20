
The qCrawl settings allows you to customize the behaviour of all the components. The settings can be populated
through different mechanisms, which are described below.

For middleware-specific settings, refer to the respective [middleware documentation](middlewares.md).

## Configuration precedence
qCrawl has the following precedence order for applying settings:

``` mermaid
flowchart LR
    A(qCrawl defaults) --> B(YML Config file) --> C(Environment variables) --> D(CLI) --> E(Programmatic overrides)
```

## Best practices
qCrawls defaults are not supposed to be changed for per-project needs. Instead, use the configuration layers
as intended:


### YML Config file
* Use a config file (e.g., `config.yaml`) for project-wide reproducible settings.
* Store non-sensitive settings like queue backend type, concurrency limits, timeouts.
* Load config file via `Settings.load(config_file="config.yaml")`.

Example usage:
```yaml title="config.yaml"
queue_backend: redis
queue_url: redis://localhost:6379/0
queue_key: qcrawl:queue
concurrency: 50
delay_per_domain: 0.1
log_level: DEBUG
```

### Environment variables
Use environment variables for deployment/CI values and secrets.

Example usage:
```bash
export QCRAWL_QUEUE_URL="redis://mypassword@redis-server:6379/0"
export QCRAWL_LOG_LEVEL="INFO"
```

!!! warning

    Never commit secrets into repository config files.


### CLI
Use CLI arguments for CI test jobs or quick overrides for one-off runs.

Example usage:
```bash
qcrawl quotes_spider:QuotesSpider \
  --concurrency 10 \
  --output output.json \
  --format json
```

!!! warning

    CLI args may appear in process lists exposing sensitive data.


### Programmatic / per-spider
Use per-spider class attributes, constructor args, or `custom_settings` for fine-grained behavior.

Example usage:
```python
class MySpider(Spider):
    name = "my_spider"

    custom_settings = {
        "concurrency": 10,
        "fingerprint_algorithm": "sha256",
        "default_headers": {
            "User-Agent": "qCrawl/1.0"
        }
    }
```

## Settings reference

### Queue settings
| Setting          | Type   | Default          | Env variable           | Validation              |
|------------------|--------|------------------|------------------------|-------------------------|
| `queue_backend`  | `str`  | `memory`         | `QCRAWL_QUEUE_BACKEND` | [`'memory'`, `'redis'`] |
| `queue_url`      | `str ` | `None`           | `QCRAWL_QUEUE_URL`     |                         |
| `queue_key`      | `str`  | `'qcrawl:queue'` | `QCRAWL_QUEUE_KEY`     |                         |
| `queue_maxsize`  | `int ` | `None`           | `QCRAWL_QUEUE_MAXSIZE` | must be >= 0 or null    |
| `queue_username` | `str`  | `None`           | `QCRAWL_QUEUE_USER`    |                         |
| `queue_password` | `str`  | `None`           | `QCRAWL_QUEUE_PASS`    |                         |


### Spider settings
| Setting                  | Type       | Default        | Env variable                     | Validation          |
|--------------------------|------------|----------------|----------------------------------|---------------------|
| `concurrency`            | `int`      | `10`           | `QCRAWL_CONCURRENCY`             | must be 1-10000     |
| `concurrency_per_domain` | `int`      | `2`            | `QCRAWL_CONCURRENCY_PER_DOMAIN`  | must be >= 1        |
| `delay_per_domain`       | `float`    | `0.25`         | `QCRAWL_DELAY_PER_DOMAIN`        | must be >= 0        |
| `max_depth`              | `int`      | `None`         | `QCRAWL_MAX_DEPTH`               |                     |
| `timeout`                | `float`    | `30.0`         | `QCRAWL_TIMEOUT`                 | must be > 0         |
| `max_retries`            | `int`      | `3`            | `QCRAWL_MAX_RETRIES`             | must be >= 0        |
| `user_agent`             | `str`      | `'qCrawl/1.0'` | `QCRAWL_USER_AGENT`              |                     |
| `ignore_query_params`    | `set[str]` | `None`         | `QCRAWL_IGNORE_QUERY_PARAMS`     | mutually exclusive  |
| `keep_query_params`      | `set[str]` | `None`         | `QCRAWL_KEEP_QUERY_PARAMS`       | mutually exclusive  |


### Logging settings
| Setting     | Type   | Default  | Env variable        | Validation                                         |
|-------------|--------|----------|---------------------|----------------------------------------------------|
| `log_level` | `str`  | `'INFO'` | `QCRAWL_LOG_LEVEL`  | `['DEBUG', 'INFO', 'WARNING, 'ERROR, 'CRITICAL']`  |
| `log_file`  | `str`  | `None`   | `QCRAWL_LOG_FILE`   | `str`                                              |
