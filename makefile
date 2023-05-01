# Makefile

# Define variables
PYTHON := python3
PIP := $(PYTHON) -m pip
PYTEST := pytest
PYTEST_OPTS :=
PYLINT := pylint
PYLINT_OPTS := --rcfile=.pylintrc
REQS := requirements.txt

# Default target
all: 
	make install-dependencies
	make pylint
	make pytest

# Install dependencies from requirements.txt
install-dependencies:
	$(PIP) install --user -r $(REQS)

# Run pylint on all Python files
pylint:
	$(PYLINT) $(PYLINT_OPTS) *.py

# Run tests using pytest
pytest:
	$(PYTHON) -m $(PYTEST) $(PYTEST_OPTS)