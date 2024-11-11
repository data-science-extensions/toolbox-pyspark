#* Variables
PYTHON := python3
PACKAGE_NAME := toolbox_pyspark
PYTHONPATH := `pwd`
VERSION ?= v0.0.0
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


#* Python
.PHONY: prepare-python
install-python:
	sudo apt-get install python3-venv --yes
install-pip:
	sudo apt-get install python3-pip --yes
upgrade-pip:
	$(PYTHON) -m pip install --upgrade pip
install-python-and-pip: install-python install-pip upgrade-pip


#* Poetry
.PHONY: poetry-installs
install-poetry:
	$(PYTHON) -m pip install poetry
	poetry --version
install:
	poetry install --no-interaction --only main
install-dev:
	poetry install --no-interaction --with dev
install-docs:
	poetry install --no-interaction --with docs
install-test:
	poetry install --no-interaction --with test
install-dev-test:
	poetry install --no-interaction --with dev,test
install-all:
	poetry install --no-interaction --with dev,docs,test
