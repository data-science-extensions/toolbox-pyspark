pip install --upgrade pip pipenv poetry
poetry --version
poetry config virtualenvs.create true
poetry config virtualenvs.in-project true
poetry config --list
poetry init --no-interaction --name="toolbox-python" --description="Helper files/functions/classes for generic PySpark processes" --author="Admin <toolbox-python@data-science-extensions.com>" --python=">3.9,<4.0" --license="MIT"
poetry env use $(pyenv which python 3.13)
poetry add "typeguard==4.*"
poetry add $(cat requirements/root.txt)
poetry add --group="dev" $(cat requirements/dev.txt)
poetry add --group="test" $(cat requirements/test.txt)
poetry add --group="docs" $(cat requirements/docs.txt)
poetry lock
poetry install --no-interaction --with dev,docs,test
poetry shell
pre-commit install
pre-commit autoupdate
pre-commit validate-config
