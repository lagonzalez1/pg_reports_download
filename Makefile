# Makefile at project root

PYTHON := python3
PYTEST := pytest

TEST_DIR := Parser/test
TEST_FILE_TUTOR_PARSER := $(TEST_DIR)/test_tutor_parser.py
TEST_FILE_STUDENT_PARSER := $(TEST_DIR)/test_student_parser.py

.PHONY: help test lint clean venv

help:
	@echo "Available targets:"
	@echo "  make test     - run unit tests with pytest"
	@echo "  make lint     - run flake8 lint checks"
	@echo "  make clean    - remove Python cache/__pycache__ files"
	@echo "  make venv     - create virtual environment"

# Run tests (will install pytest if missing)
test:
	@echo "Running tests in $(TEST_FILE)"
	@$(PYTHON) -m pip install -q pytest
	@$(PYTHON) -m $(PYTEST) $(TEST_FILE_TUTOR_PARSER) -v
	@$(PYTHON) -m $(PYTEST) $(TEST_FILE_STUDENT_PARSER) -v

# Run lint checks (optional)
lint:
	@$(PYTHON) -m pip install -q flake8
	@$(PYTHON) -m flake8 .

# Clean cache
clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete
	@find . -type f -name "*.pyo" -delete

# Create virtual environment (default .venv folder)
venv:
	@$(PYTHON) -m venv .venv
	@echo "Virtual environment created in .venv/"