
The qCrawl settings allows you to customize the behaviour of all the components. The settings can be populated
through different mechanisms, which are described below.

For middleware-specific settings, refer to the respective [middleware documentation](middlewares.md).

## Configuration precedence
qCrawl has the following precedence order for applying settings:

``` mermaid
flowchart LR
    A(qCrawl defaults) --> B(TOML Config file) --> C(Environment variables) --> D(CLI) --> E(Programmatic overrides)
```

## Best practices
qCrawl's defaults are not supposed to be changed for per-project needs. Instead, use the configuration layers
as intended:


### TOML Config file
* Use a config file (e.g., `config.toml`) for project-wide reproducible settings.
* Store non-sensitive settings like queue backend type, concurrency limits, timeouts.
* Load config file via `Settings.load(config_file="config.toml")`.

Example usage:
```toml title="config.toml"
# Use: runtime_settings = Settings.load(config_file="config.toml")

CONCURRENCY = 20
CONCURRENCY_PER_DOMAIN = 4
DELAY_PER_DOMAIN = 0.5
TIMEOUT = 45.0
MAX_RETRIES = 5
USER_AGENT = "MyCrawler/1.0"

QUEUE_BACKEND = "disk"  # or "memory", "redis"

[QUEUE_BACKENDS.disk]
class = "qcrawl.core.queues.disk.DiskQueue"
path = ""  # Empty/null = system temp dir (cross-platform)
maxsize = 0  # Max requests count (0 = unlimited)
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

# Queue backend selection
export QCRAWL_QUEUE_BACKEND="disk"  # or "memory", "redis"

# Disk queue configuration (optional)
export QCRAWL_QUEUE_BACKENDS__disk__path="/var/qcrawl/queue"
export QCRAWL_QUEUE_BACKENDS__disk__maxsize="10000"  # Max requests count
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
  -s USER_AGENT="MyCrawler/1.0" \
  -s QUEUE_BACKEND=disk \
  -s QUEUE_BACKENDS.disk.path=/tmp/my_queue \
  -s QUEUE_BACKENDS.disk.maxsize=10000
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
        "QUEUE_BACKEND": "disk",
        "QUEUE_BACKENDS": {
            "disk": {
                "path": "/tmp/my_spider_queue",
                "maxsize": 10000,  # Max requests count
            }
        },
    }

    async def parse(self, response):
        ...
```

## Settings reference

### Queue settings
| Setting           | Type    | Default    | Env variable            | Notes                                                                   |
|-------------------|---------|------------|-------------------------|-------------------------------------------------------------------------|
| `QUEUE_BACKEND`   | `str`   | `memory`   | `QCRAWL_QUEUE_BACKEND`  | Backend to use: `memory`, `disk`, `redis`, or custom                    |
| `QUEUE_BACKENDS`  | `dict`  | see below  | `QCRAWL_QUEUE_BACKENDS` | Mapping of backend name → backend config (see structure below)          |

**QUEUE_BACKENDS structure:**

```toml
QUEUE_BACKEND = "memory"  # or "disk", "redis"

[QUEUE_BACKENDS.memory]
class = "qcrawl.core.queues.memory.MemoryPriorityQueue"
maxsize = 0  # Max requests count (0 = unlimited)

[QUEUE_BACKENDS.disk]
class = "qcrawl.core.queues.disk.DiskQueue"
path = ""  # Empty/null = system temp dir (cross-platform)
maxsize = 0  # Max requests count (0 = unlimited)

[QUEUE_BACKENDS.redis]
class = "qcrawl.core.queues.redis.RedisQueue"
# url = ""  # optional full connection URL (overrides host/port/user/password)
host = "localhost"
port = "6379"
user = "user"
password = "pass"
namespace = "qcrawl"
ssl = false
maxsize = 0  # Max requests count (0 = unlimited)
dedupe = false
update_priority = false
fingerprint_size = 16
item_ttl = 86400  # seconds, 0 = no expiration
dedupe_ttl = 604800  # seconds, 0 = no expiration
max_orphan_retries = 10
redis_kwargs = {}  # driver-specific options passed to redis client
```

!!! info "Redis version compatibility"

    **Redis server 7.4+ recommended** for full TTL support (per-item expiration)

    - **With Redis 7.4+**: All features supported, including `item_ttl` and `dedupe_ttl` per-item expiration
    - **With Redis 6.x**: Basic queue operations work, but TTL features (`item_ttl`, `dedupe_ttl`) will fail with command errors (`HEXPIRE` not available)

### Spider settings
| Setting                    | Type       | Default        | Env variable                     | Validation                               |
|----------------------------|------------|----------------|----------------------------------|------------------------------------------|
| `CONCURRENCY`              | `int`      | `10`           | `QCRAWL_CONCURRENCY`             | must be 1-10000                          |
| `CONCURRENCY_PER_DOMAIN`   | `int`      | `2`            | `QCRAWL_CONCURRENCY_PER_DOMAIN`  | must be >= 1, cannot exceed CONCURRENCY  |
| `DELAY_PER_DOMAIN`         | `float`    | `0.25`         | `QCRAWL_DELAY_PER_DOMAIN`        | must be >= 0                             |
| `MAX_DEPTH`                | `int`      | `0`            | `QCRAWL_MAX_DEPTH`               |                                          |
| `TIMEOUT`                  | `float`    | `30.0`         | `QCRAWL_TIMEOUT`                 | must be > 0                              |
| `MAX_RETRIES`              | `int`      | `3`            | `QCRAWL_MAX_RETRIES`             | must be >= 0                             |
| `USER_AGENT`               | `str`      | `'qCrawl/1.0'` | `QCRAWL_USER_AGENT`              |                                          |
| `IGNORE_QUERY_PARAMS`      | `set[str]` | `None`         | `QCRAWL_IGNORE_QUERY_PARAMS`     | mutually exclusive                       |
| `KEEP_QUERY_PARAMS`        | `set[str]` | `None`         | `QCRAWL_KEEP_QUERY_PARAMS`       | mutually exclusive                       |


### Logging settings
| Setting          | Type  | Default                                             | Env variable            | Validation                                          |
|------------------|-------|-----------------------------------------------------|-------------------------|-----------------------------------------------------|
| `LOG_LEVEL`      | `str` | `'INFO'`                                            | `QCRAWL_LOG_LEVEL`      | `['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']` |
| `LOG_FILE`       | `str` | `None`                                              | `QCRAWL_LOG_FILE`       |                                                     |
| `LOG_FORMAT`     | `str` | `'%(asctime)s %(levelname)s %(name)s: %(message)s'` | `QCRAWL_LOG_FORMAT`     |                                                     |
| `LOG_DATEFORMAT` | `str` | `None`                                              | `QCRAWL_LOG_DATEFORMAT` |                                                     |
