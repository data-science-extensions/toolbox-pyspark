#* Variables
PYTHON := python3
PACKAGE_NAME := toolbox_pyspark
PACKAGE_DESCRIPTION := "Helper files/functions/classes for generic PySpark processes"
PYTHONPATH := `pwd`
VERSION ?= v0.0.0  #<-- Default version if not already set by the CI/CD workflows
MINIMUM_COVERAGE ?= 99  #<-- Default minimum coverage if not already set by the CI/CD workflows
VERSION_CLEAN := $(shell echo $(VERSION) | awk '{gsub(/v/,"")}1')
VERSION_NO_PATCH := "$(shell echo $(VERSION) | cut --delimiter '.' --fields 1-2).*"


#* Environment
.PHONY: check-environment
update-build-essentials:
	sudo apt-get install build-essential
update-environment:
	sudo apt-get update --yes
	sudo apt-get upgrade --yes
install-git:
	sudo apt-get install git --yes
install-java:
	sudo apt-get install default-jdk --yes
check-java-available:
	java -version
	javac -version
check-java-installed: check-java-available
install-check-java: install-java check-java-installed


#* Python
.PHONY: prepare-python
install-python:
	sudo apt-get install python3-venv --yes
install-pip:
	sudo apt-get install python3-pip --yes
install-package-managers:
	pip install pip pipenv poetry
	pip install --upgrade pip pipenv poetry
upgrade-pip:
	pip install --upgrade pip
install-python-and-pip: install-python install-pip upgrade-pip


#* Poetry
.PHONY: poetry-installs
install-poetry:
	pip install poetry
	poetry --version
	poetry config virtualenvs.create true
	poetry config virtualenvs.in-project true
	poetry config --list
poetry-lock:
	poetry lock
poetry-check:
	poetry check
install:
	poetry lock
	poetry install --no-interaction --only main
install-dev:
	poetry lock
	poetry install --no-interaction --with dev
install-docs:
	poetry lock
	poetry install --no-interaction --with docs
install-test:
	poetry lock
	poetry install --no-interaction --with test
install-dev-test:
	poetry lock
	poetry install --no-interaction --with dev,test
install-all:
	poetry lock
	poetry install --no-interaction --with dev,docs,test
update-packages: install-all poetry-check


#* Initialisation
initialise-poetry:
	poetry --version
	poetry config virtualenvs.create true
	poetry config virtualenvs.in-project true
	poetry config --list
	poetry init --no-interaction --name="$(PACKAGE_NAME)" --description="$(PACKAGE_DESCRIPTION)" --author="Admin <toolbox-python@data-science-extensions.com>" --python=">3.9,<4.0" --license="MIT"
initialise-poetry-packages:
	poetry add "typeguard==4.*"
	poetry add $(cat requirements/root.txt)
	poetry add --group="dev" $(cat requirements/dev.txt)
	poetry add --group="test" $(cat requirements/test.txt)
	poetry add --group="docs" $(cat requirements/docs.txt)
	poetry lock
initialise-pre-commit:
	poetry run pre-commit install
	poetry run pre-commit autoupdate
	poetry run pre-commit validate-config
	poetry install --no-interaction --with dev,docs,test
initialise-local-environment: install-package-managers initialise-poetry initialise-poetry-packages initialise-pre-commit


#* Linting
.PHONY: linting
run-black:
	poetry run black --config pyproject.toml ./
run-isort:
	poetry run isort --settings-file pyproject.toml ./
run-safety:
	poetry check
lint: run-black run-isort run-safety


#* Checking
.PHONY: checking
check-black:
	poetry run black --diff --check --config pyproject.toml ./
check-mypy:
	poetry run mypy --install-types --non-interactive --config-file pyproject.toml src/$(PACKAGE_NAME)
check-isort:
	poetry run isort --settings-file pyproject.toml ./
check-codespell:
	poetry run codespell --toml pyproject.toml src/ *.py
check-safety:
	poetry check
check-pylint:
	poetry run pylint --rcfile=pyproject.toml src/$(PACKAGE_NAME)
check-pytest:
	poetry run pytest --config-file pyproject.toml
check-pycln:
	poetry run pycln --config="pyproject.toml" src/$(PACKAGE_NAME)
check-mkdocs:
	poetry run mkdocs build --site-dir="temp"
	if [ -d "temp" ]; then rm -rf temp; fi
check-complexity:
	poetry run radon cc --show-complexity --total-average ./src/$(PACKAGE_NAME)
check: check-black check-pycln check-isort check-codespell check-pylint check-complexity check-mkdocs check-pytest
# check: check-black check-mypy check-pycln check-isort check-codespell check-pylint check-mkdocs check-pytest
lint-check: lint check


#* Testing
.PHONY: pytest
pytest:
	poetry run pytest --config-file pyproject.toml
copy-coverage-report:
	cp --recursive --update "./cov-report/html/." "./docs/code/coverage/"
commit-coverage-report:
	git add .
	git commit --no-verify --message "Update coverage report [skip ci]"
	git push
assert-coverage:
	poetry run coverage report --fail-under=$(MINIMUM_COVERAGE)


#* Git
.PHONY: git-processes
git-add-credentials-old:
	git config --global user.name ${GITHUB_ACTOR}
	git config --global user.email "${GITHUB_ACTOR}@users.noreply.github.com"
git-add-credentials:
	git config --global user.name "github-actions[bot]"
	git config --global user.email "github-actions[bot]@users.noreply.github.com"
configure-git: git-add-credentials
git-refresh-current-branch:
	git remote update
	git fetch --verbose
	git fetch --verbose --tags
	git pull  --verbose
	git status --verbose
	git branch --list --verbose
	git tag --list --sort=-creatordate
git-switch-to-main-branch:
	git checkout -B main --track origin/main
git-switch-to-docs-branch:
	git checkout -B docs-site --track origin/docs-site


#* Changelog
.PHONY: changelog
build-changelog:
	chmod +x ./src/cli/changelog.sh
	./src/cli/changelog.sh > ./CHANGELOG.md
commit-changelog:
	git add ./CHANGELOG.md
	git commit --message="Update changelog [skip ci]" --no-verify
	git push --force --no-verify
	git status


#* Deploy Package
# See: https://github.com/monim67/poetry-bumpversion
.PHONY: deployment
bump-version:
	poetry self add poetry-bumpversion
	poetry version $(VERSION_CLEAN)
	poetry version --short
update-git:
	git add .
	git commit --message="Bump to version \`$(VERSION)\` [skip ci]" --allow-empty
	git push --force --no-verify
	git status
poetry-build:
	poetry build
poetry-configure:
	poetry config pypi-token.pypi ${PYPI_TOKEN}
poetry-publish:
	poetry publish
build-package: poetry-build
deploy-package: poetry-configure poetry-publish


#* Docs
.PHONY: docs
docs-serve-static:
	poetry run mkdocs serve
docs-serve-versioned:
	poetry run mike serve --branch=docs-site
docs-build-static:
	poetry run mkdocs build --clean
docs-build-versioned:
	git config --global --list
	git config --local --list
	git remote -v
	poetry run mike --debug deploy --update-aliases --branch=docs-site --push $(VERSION) latest
update-git-docs:
	git add .
	git commit -m "Build docs [skip ci]"
	git push --force --no-verify --push-option ci.skip
docs-check-versions:
	poetry run mike --debug list --branch=docs-site
docs-delete-version:
	poetry run mike --debug delete --branch=docs-site $(VERSION)
docs-set-default:
	poetry run mike --debug set-default --branch=docs-site --push latest
build-static-docs: docs-build-static update-git-docs
build-versioned-docs: docs-build-versioned docs-set-default
