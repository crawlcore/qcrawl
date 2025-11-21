
The qCrawl settings allows you to customize the behaviour of all the components. The settings can be populated
through different mechanisms, which are described below.

For middleware-specific settings, refer to the respective [middleware documentation](middlewares.md).

## Configuration precedence
qCrawl has the following precedence order for applying settings:

``` mermaid
flowchart LR
    A(qCrawl defaults) --> B(YAML Config file) --> C(Environment variables) --> D(CLI) --> E(Programmatic overrides)
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
# Use: runtime_settings = Settings.load(config_file="config.yaml")

CONCURRENCY: 20
CONCURRENCY_PER_DOMAIN: 4
DELAY_PER_DOMAIN: 0.5
TIMEOUT: 45.0
MAX_RETRIES: 5
USER_AGENT: "MyCrawler/1.0"
```

### Environment variables
Use environment variables for deployment/CI values and secrets.

Example usage:
```bash
export QCRAWL_CONCURRENCY="20"
export QCRAWL_CONCURRENCY_PER_DOMAIN="4"
export QCRAWL_DELAY_PER_DOMAIN="0.5"
export QCRAWL_TIMEOUT="45.0"
export QCRAWL_MAX_RETRIES="5"
export QCRAWL_USER_AGENT="MyCrawler/1.0"
```

!!! warning

    Never commit secrets into repository config files.


### CLI
Use CLI arguments for CI test jobs or quick overrides for one-off runs.

Example usage:
```bash
qcrawl mypackage.spiders:QuotesSpider \
  -s CONCURRENCY=20 \
  -s CONCURRENCY_PER_DOMAIN=4 \
  -s DELAY_PER_DOMAIN=0.5 \
  -s TIMEOUT=45.0 \
  -s MAX_RETRIES=5 \
  -s USER_AGENT="MyCrawler/1.0"
```

!!! warning

    CLI args may appear in process lists exposing sensitive data.


### Programmatic / per-spider
Use per-spider class attributes, constructor args, or `custom_settings` for fine-grained behavior.

Example usage:
```python
from qcrawl.core.spider import Spider

class MySpider(Spider):
    name = "my_spider"
    start_urls = ["https://example.com"]

    custom_settings = {
        "CONCURRENCY": 20,
        "CONCURRENCY_PER_DOMAIN": 4,
        "DELAY_PER_DOMAIN": 0.5,
        "TIMEOUT": 45.0,
        "MAX_RETRIES": 5,
        "USER_AGENT": "MyCrawler/1.0",
    }

    async def parse(self, response):
        ...
```

## Settings reference

### Queue settings
| Setting           | Type    | Default    | Notes                                                                         |
|-------------------|---------|------------|-------------------------------------------------------------------------------|
| `QUEUE_BACKEND`   | `str`   | `memory`   | Set which backend from `QUEUE_BACKENDS` (`memory`, `redis`, or custom) to use |
| `QUEUE_BACKENDS`  | `dict`  | see below  | Mapping of backend name → backend config template                             |

```yaml
QUEUE_BACKENDS:
  memory:
    class: "qcrawl.core.queues.memory.MemoryPriorityQueue"
    maxsize: 0 # 0 = unlimited

  redis:
    class: "qcrawl.core.queues.redis.RedisQueue"
    #url: null # optional full connection URL (overrides host/port/user/password)
    host: "localhost"
    port: "6379"
    user: "user"
    password: "pass"
    namespace: "qcrawl"
    ssl: false
    maxsize: 0 # 0 = unlimited
    dedupe: false
    update_priority: false
    fingerprint_size: 16
    item_ttl: 86400 # seconds, 0 = no expiration
    dedupe_ttl: 604800 # seconds, 0 = no expiration
    max_orphan_retries: 10
    redis_kwargs: {} # driver-specific options passed to redis client
```

### Spider settings
| Setting                  | Type       | Default        | Env variable                     | Validation          |
|--------------------------|------------|----------------|----------------------------------|---------------------|
| `concurrency`            | `int`      | `10`           | `QCRAWL_CONCURRENCY`             | must be 1-10000     |
| `concurrency_per_domain` | `int`      | `2`            | `QCRAWL_CONCURRENCY_PER_DOMAIN`  | must be >= 1        |
| `delay_per_domain`       | `float`    | `0.25`         | `QCRAWL_DELAY_PER_DOMAIN`        | must be >= 0        |
| `max_depth`              | `int`      | `0`            | `QCRAWL_MAX_DEPTH`               |                     |
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
