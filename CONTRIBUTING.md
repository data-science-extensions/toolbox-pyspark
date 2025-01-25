# Contributing

Contribution is always welcome.

1. First, either [fork][github-fork] or [branch][github-branch] the [main repo][github-repo].

2. [Clone][github-clone] your forked/branched repo.

3. Build your environment:

    1. With [`pipenv`][pipenv] on Windows:

        ```pwsh
        if (-not (Test-Path .venv)) {mkdir .venv}
        python -m pipenv install --requirements requirements.txt --requirements requirements-dev.txt --skip-lock
        python -m poetry run pre-commit install
        python -m poetry run pre-commit autoupdate
        python -m poetry shell
        ```

    2. With [`pipenv`][pipenv] on Linux:

        ```sh
        mkdir .venv
        python3 -m pipenv install --requirements requirements.txt --requirements requirements-dev.txt --skip-lock
        python3 -m poetry run pre-commit install
        python3 -m poetry run pre-commit autoupdate
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
        python -m poetry run pre-commit autoupdate
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
        python3 -m poetry run pre-commit autoupdate
        python3 -m poetry shell
        ```

    5. With [`uv`][uv] on Windows:

        ```pwsh
        powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
        uv install python 3.13
        uv sync --all-groups
        uv run pre-commit install
        uv run pre-commit autoupdate
        ```

    6. With [`uv`][uv] on Linux:

        ```sh
        curl -LsSf https://astral.sh/uv/install.sh | sh
        uv install python 3.13
        uv sync --all-groups
        uv run pre-commit install
        uv run pre-commit autoupdate
        ```

4. Start contributing.

5. When you're happy with the changes, raise a [Pull Request][github-pr] to merge with the [main][github-repo] branch again.


### Build and Test

To ensure that the package is working as expected, please ensure that:

1. You write your code as per [PEP8][pep8] requirements (lint using [Black][black]).
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

[github-fork]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo
[github-branch]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-branches
[github-repo]: https://github.com/data-science-extensions/toolbox-pyspark
[github-clone]: https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository
[pipenv]: https://github.com/pypa/pipenv
[poetry]: https://python-poetry.org
[uv]: https://docs.astral.sh/uv/
[github-pr]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests
[pep8]: https://peps.python.org/pep-0008/
[unittest]: https://docs.python.org/3/library/unittest.html
[codecov]: https://codecov.io/
[pytest]: https://docs.pytest.org
[mypy]: https://www.mypy-lang.org/
[black]: https://black.readthedocs.io/
