# Detect OS
ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := Linux
endif


# LINTERS

pylint:
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;34mRunning pylint...\033[0m"
	@start=$$(date +%s.%N); \
    # Fiz nesse esquema pra sempre pegar as pastas novas que forem criadas no repo
	@pylint $$(find . -type f -name "*.py" ! -path "./.venv/*" ! -path "./htmlcov/*" ! -path "./.pytest_cache/*" ! -path "./.ruff_cache/*") --output=.pylint-report.txt; end=$$(date +%s.%N); \
	runtime=$$(echo "$$end - $$start" | bc); \
	printf "\033[1;32mPylint report generated at ./.pylint-report.txt - (executed in %.3f seconds)\n\033[0m" $$runtime
else
	@echo Running pylint...
	@pylint . --ignore=.venv --output=.pylint-report.txt
	@echo Pylint report generated at ./.pylint-report.txt
endif

ruff:
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;34mRunning ruff check...\033[0m"
	@start=$$(date +%s); \
	ruff check . --output-file=.ruff-report.txt; \
	end=$$(date +%s); \
	runtime=$$((end-start)); \
	echo "\033[1;32mRuff report generated at ./.ruff-report.txt (executed in $$runtime seconds)\033[0m"
else
	@echo Running ruff check...
	@ruff check . --output-file=.ruff-report.txt
	@echo Ruff report generated at ./.ruff-report.txt
endif

markdown-lint:
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;34mRunning pymarkdownlnt...\033[0m"
	@start=$$(date +%s); \
	pymarkdownlnt --config .pymarkdown.json scan . > .markdown-report.txt 2>&1 || true; \
	end=$$(date +%s); \
	runtime=$$((end-start)); \
	echo "\033[1;32mMarkdown lint report generated at ./.markdown-report.txt (executed in $$runtime seconds)\033[0m"
else
	@echo Running pymarkdownlnt...
	@pymarkdownlnt --config .pymarkdown.json scan . > .markdown-report.txt 2>&1
	@echo Markdown lint report generated at ./.markdown-report.txt
endif

lint: ruff markdown-lint pylint


# FORMATTERS

format-ruff:
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;34mRunning ruff to sort imports...\033[0m"
	@ruff check --select I --fix .
	@echo "\033[1;34mRunning ruff to format code...\033[0m"
else
	@echo Running ruff to sort imports...
	@ruff check --select I --fix .
	@echo Running ruff to format code...
endif
	@ruff format .
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;32mRuff formatting completed.\033[0m"
else
	@echo Ruff formatting completed.
endif

format-terraform:
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;34mRunning terraform fmt to format Terraform files...\033[0m"
	@if command -v terraform > /dev/null 2>&1; then terraform fmt -recursive terraform; echo "\033[1;32mTerraform formatting completed.\033[0m"; else echo "\033[1;33mterraform not found, skipping format-terraform.\033[0m"; fi
else
	@echo Running terraform fmt to format Terraform files...
	@where terraform >nul 2>&1 && terraform fmt -recursive terraform && echo Terraform formatting completed. || echo terraform not found, skipping format-terraform.
endif

format-markdown:
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;34mRunning mdformat...\033[0m"
else
	@echo Running mdformat...
endif
	@mdformat .
ifeq ($(PLATFORM),Linux)
	@echo "\033[1;32mMarkdown formatting completed.\033[0m"
else
	@echo Markdown formatting completed.
endif

format: format-ruff format-terraform format-markdown


# TESTS

test:
	pytest
coverage:
	pytest --cov=maggulake
coverage-html:
	pytest --cov=maggulake --cov-report=html


# INSTALLATION

venv:
ifeq ($(PLATFORM),Windows)
	@python -m venv .venv
	@.venv\Scripts\activate
else
	@python3 -m venv .venv
	@source .venv/bin/activate
endif


install:
	@echo "Instalando dependencias do Python..."
	@pip install -r requirements.in
	@pip install -r requirements-dev.in
	@echo "Instalando o pacote maggulake em modo editavel..."
	@pip install -e .
