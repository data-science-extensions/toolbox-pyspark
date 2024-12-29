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

You can install and use this package multiple ways by using [`pip`][pip], [`pipenv`][pipenv], or [`poetry`][poetry].


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
    [tool.poetry.dependencies]
    toolbox-pyspark = "*"
    ```

    Then run:

    ```sh
    poetry install
    ```

2. Or just run this:

    ```sh
    poetry add toolbox-pyspark
    poetry install
    poetry sync
    ```


### Contribution

Contribution is always welcome.

1. First, either [fork][github-fork] or [branch][github-branch] the [main repo][github-repo].

2. [Clone][github-clone] your forked/branched repo.

3. Build your environment:

    1. With [`pipenv`][pipenv] on Windows:

        ```pwsh
        if (-not (Test-Path .venv)) {mkdir .venv}
        python -m pipenv install --requirements requirements.txt --requirements requirements-dev.txt --skip-lock
        python -m poetry run pre-commit install
        python -m poetry shell
        ```

    2. With [`pipenv`][pipenv] on Linux:

        ```sh
        mkdir .venv
        python3 -m pipenv install --requirements requirements.txt --requirements requirements-dev.txt --skip-lock
        python3 -m poetry run pre-commit install
        python3 -m poetry shell
        ```

    3. With [`poetry`][poetry] on Windows:

        ```pwsh
        python -m pip install --upgrade pip
        python -m pip install poetry
        python -m poetry init
        python -m poetry add $(cat requirements/root.txt)
        python -m poetry add --group=dev $(cat requirements/dev.txt)
        python -m poetry add --group=test $(cat requirements/test.txt)
        python -m poetry add --group=docs $(cat requirements/docs.txt)
        python -m poetry install
        python -m poetry run pre-commit install
        python -m poetry shell
        ```

    4. With [`poetry`][poetry] on Linux:

        ```sh
        python3 -m pip install --upgrade pip
        python3 -m pip install poetry
        python3 -m poetry init
        python3 -m poetry add $(cat requirements/root.txt)
        python3 -m poetry add --group=dev $(cat requirements/dev.txt)
        python3 -m poetry add --group=test $(cat requirements/test.txt)
        python3 -m poetry add --group=docs $(cat requirements/docs.txt)
        python3 -m poetry install
        python3 -m poetry run pre-commit install
        python3 -m poetry shell
        ```

4. Start contributing.

5. When you're happy with the changes, raise a [Pull Request][github-pr] to merge with the [main][github-repo] branch again.


### Build and Test

To ensure that the package is working as expected, please ensure that:

1. You write your code as per [PEP8][pep8] requirements.
2. You write a [UnitTest][unittest] for each function/feature you include.
3. The [CodeCoverage][codecov] is 100%.
4. All [UnitTests][pytest] are passing.
5. [MyPy][mypy] is passing 100%.


#### Testing

- Run them all together

    ```sh
    poetry run make check
    ```

- Or run them individually:

    - [Black][black]
        ```pysh
        poetry run make check-black
        ```

    - [PyTests][pytest]:
        ```sh
        poetry run make ckeck-pytest
        ```

    - [MyPy][mypy]:
        ```sh
        poetry run make check-mypy
        ```


[github-repo]: https://github.com/data-science-extensions/toolbox-pyspark
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
[github-fork]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo
[github-branch]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-branches
[github-clone]: https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository
[github-pr]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests
[pep8]: https://peps.python.org/pep-0008/
[unittest]: https://docs.python.org/3/library/unittest.html
[codecov]: https://codecov.io/
[pytest]: https://docs.pytest.org
[mypy]: https://www.mypy-lang.org/
[black]: https://black.readthedocs.io/
