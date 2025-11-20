
## Requirements

### Python
qCrawl requires Python 3.11+, either the CPython implementation (default) or the PyPy implementation (see [Alternate Implementations](https://docs.python.org/3/reference/introduction.html#implementations)).

I suggest to use CPython:

* Compatibility: qCrawl stack relies on C-extensions (orjson, lxml, pickle wheels). They require nontrivial build workarounds.
* Workload profile: qCrawl is I/O-bound (networking via aiohttp, disk I/O, async queueing). PyPy’s JIT gives little advantage for async I/O where most time is spent waiting in C libraries or the OS.
* Ecosystem: aiohttp and other async libraries are written and optimized for CPython. Wheels and CI support are more reliable on CPython.

### Dependencies
qCrawl is written in pure python and depends on a several key Python libraries (among others):

1. [aiohttp](https://docs.aiohttp.org/en/stable/index.html), high-performance asynchronous HTTP client.
2. [lxml](https://lxml.de/index.html), an efficient XML and HTML parser.
3. [yarl](https://yarl.aio-libs.org/en/latest/), an efficient URL parser and builder.
4. [orjson](https://github.com/ijl/orjson), fast JSON library for parsing and serialization.

Some of these packages themselves depend on non-Python packages that might require additional installation steps depending on your platform.

## Installation
Python packages can be installed either globally (system-wide installation), or in user-space using so-called *virtual environment*.

Generally, I suggest to install qCrawl inside a [virtual environment](https://docs.python.org/3/library/venv.html#module-venv) on all platforms.
Virtual environments allow you to avoid conflicts with already-installed Python system packages, and still install packages normally with `pip`.

### Linux/MacOS
qCraw depends on popular packages and prebuilt wheels should be available for them on MacOS and Linux.
Therefore once you have virtual environment set up and activated, try to install qCrawl using:

``` shell
pip install qcrawl
```

If you encounter issues installing the dependencies install following system packages first to build dependencies from the source:

```shell title="Linux (Debian/Ubuntu)"
# install compilers & headers required for building C/Rust extensions
sudo apt update
sudo apt install -y build-essential libssl-dev libffi-dev python3-dev libxml2-dev libxslt1-dev zlib1g-dev pkg-config

# optional: install Rust toolchain if orjson fails to install

# preferred way to get latest stable Rust (instead of apt)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# Upgrade packaging tools so pip prefers wheels
python -m pip install --upgrade pip setuptools wheel

# Install released package
python -m pip install qcrawl

```

```shell title="MacOS"
# install Xcode command-line tools
xcode-select --install

# install native libs
brew install libxml2

# optional: install Rust toolchain if orjson fails to install
brew install rust

# upgrade packaging tools before installing
python3 -m pip install --upgrade pip setuptools wheel

# if lxml installation still fails, set the following environment variables
# export LDFLAGS="-L/usr/local/opt/libxml2/lib"
# export CPPFLAGS="-I/usr/local/opt/libxml2/include"

# finally install qCrawl
python3 -m pip install qcrawl
```

### Windows
I suggest to install [Anaconda](https://www.anaconda.com/docs/getting-started/anaconda/main) (a fully loaded Python environment) or [Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) (a lightweight base installer) and use the package from the [conda-forge](https://conda-forge.org/) channel (community-maintained package repository), which will avoid most installation issues.

Once you’ve installed Anaconda or Miniconda, install qCrawl with:

``` shell
conda install -c conda-forge qcrawl
```
