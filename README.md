<h1 align="center"><u><code>toolbox-pyspark</code></u></h1>

<p align="center">
<a href="https://pypi.org/project/toolbox-pyspark">
    <img src="https://img.shields.io/pypi/implementation/toolbox-pyspark?logo=pypi&logoColor=ffde57" alt="implementation"></a>
<a href="https://pypi.org/project/toolbox-pyspark">
    <img src="https://img.shields.io/pypi/pyversions/toolbox-pyspark?logo=python&logoColor=ffde57" alt="python-versions"></a>
<a href="https://pypi.org/project/toolbox-pyspark">
    <img src="https://img.shields.io/pypi/v/toolbox-pyspark?label=version&logo=pypi&logoColor=ffde57&color=blue" alt="version"></a>
<a href="https://github.com/data-science-extensions/toolbox-pyspark/releases">
    <img src="https://img.shields.io/github/v/release/data-science-extensions/toolbox-pyspark?logo=github" alt="github-release"></a>
<br>
<a href="https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/ci.yml">
    <img src="https://img.shields.io/static/v1?label=os&message=ubuntu+|+macos+|+windows&color=blue&logo=ubuntu&logoColor=green" alt="os"></a>
<a href="https://pypi.org/project/toolbox-pyspark">
    <img src="https://img.shields.io/pypi/status/toolbox-pyspark?color=green" alt="pypi-status"></a>
<a href="https://pypi.org/project/toolbox-pyspark">
    <img src="https://img.shields.io/pypi/format/toolbox-pyspark?color=green" alt="pypi-format"></a>
<a href="https://github.com/data-science-extensions/toolbox-pyspark/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/data-science-extensions/toolbox-pyspark?color=green" alt="github-license"></a>
<a href="https://piptrends.com/package/toolbox-pyspark">
    <img src="https://img.shields.io/pypi/dm/toolbox-pyspark?color=green" alt="pypi-downloads"></a>
<a href="https://codecov.io/gh/data-science-extensions/toolbox-pyspark">
    <img src="https://codecov.io/gh/data-science-extensions/toolbox-pyspark/graph/badge.svg" alt="codecov-repo"></a>
<a href="https://github.com/psf/black">
    <img src="https://img.shields.io/static/v1?label=style&message=black&color=black&logo=windows-terminal&logoColor=white" alt="style"></a>
<br>
<a href="https://github.com/data-science-extensions/toolbox-pyspark">
    <img src="https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat" alt="contributions"></a>
<br>
<a href="https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/ci.yml">
    <img src="https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/ci.yml/badge.svg?event=pull_request" alt="CI"></a>
<a href="https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/cd.yml">
    <img src="https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/cd.yml/badge.svg?event=release" alt="CD"></a>
</p>

### Introduction

The purpose of this package is to provide some helper files/functions/classes for generic PySpark processes.


### Key URLs

For reference, these URL's are used:

| Type | Source | URL |
|---|---|---|
| Git Repo | GitHub | https://github.com/data-science-extensions/toolbox-pyspark |
| Python Package | PyPI | https://pypi.org/project/toolbox-pyspark |
| Package Docs | Pages | https://data-science-extensions.com/toolbox-pyspark |


### Installation

You can install and use this package multiple ways by using any of your preferred methods: [`pip`][pip], [`pipenv`][pipenv], [`poetry`][poetry], or [`uv`][uv].


#### Using [`pip`][pip]:

1. In your terminal, run:

    ```sh
    python3 -m pip install --upgrade pip
    python3 -m pip install toolbox-pyspark
    ```

2. Or, in your `requirements.txt` file, add:

    ```txt
    toolbox-pyspark
    ```

    Then run:

    ```sh
    python3 -m pip install --upgrade pip
    python3 -m pip install --requirement=requirements.txt
    ```


#### Using [`pipenv`][pipenv]:

1. Install using environment variables:

    In your `Pipfile` file, add:

    ```toml
    [[source]]
    url = "https://pypi.org/simple"
    verify_ssl = false
    name = "pypi"

    [packages]
    toolbox-pyspark = "*"
    ```

    Then run:

    ```sh
    python3 -m pip install pipenv
    python3 -m pipenv install --verbose --skip-lock --categories=root index=pypi toolbox-pyspark
    ```

2. Or, in your `requirements.txt` file, add:

    ```sh
    toolbox-pyspark
    ```

    Then run:

    ```sh
    python3 -m run pipenv install --verbose --skip-lock --requirements=requirements.txt
    ```

3. Or just run this:

    ```sh
    python3 -m pipenv install --verbose --skip-lock toolbox-pyspark
    ```


#### Using [`poetry`][poetry]:

1. In your `pyproject.toml` file, add:

    ```toml
    [project]
    dependencies = [
        "toolbox-pyspark==1.*",
    ]
    ```

    Then run:

    ```sh
    poetry sync
    poetry install
    ```

2. Or just run this:

    ```sh
    poetry add "toolbox-pyspark==1.*"
    poetry sync
    poetry install
    ```


#### Using [`uv`][uv]:

1. In your `pyproject.toml` file, add:

    ```toml
    [project]
    dependencies = [
        "toolbox-pyspark==1.*",
    ]
    ```

   Then run:

   ```sh
   uv sync
   ```

2. Or run this:

    ```sh
    uv add "toolbox-pyspark==1.*"
    uv sync
    ```

3. Or just run this:

    ```sh
    uv pip install "toolbox-pyspark==1.*"
    ```


### Contribution

Check the [CONTRIBUTING.md][github-contributing] file or [Contributing][docs-contributing] page.


[github-repo]: https://github.com/data-science-extensions/toolbox-pyspark
[github-contributing]: https://github.com/data-science-extensions/toolbox-pyspark/blob/main/CONTRIBUTING.md
[docs-contributing]: https://data-science-extensions.com/toolbox-pyspark/latest/usage/contributing/
[github-release]: https://github.com/data-science-extensions/toolbox-pyspark/releases
[github-ci]: https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/ci.yml
[github-cd]: https://github.com/data-science-extensions/toolbox-pyspark/actions/workflows/cd.yml
[github-license]: https://github.com/data-science-extensions/toolbox-pyspark/blob/main/LICENSE
[codecov-repo]: https://codecov.io/gh/data-science-extensions/toolbox-pyspark
[pypi]: https://pypi.org/project/toolbox-pyspark
[docs]: https://data-science-extensions.com/toolbox-pyspark
[pip]: https://pypi.org/project/pip
[pipenv]: https://github.com/pypa/pipenv
[poetry]: https://python-poetry.org
[uv]: https://docs.astral.sh/uv/
