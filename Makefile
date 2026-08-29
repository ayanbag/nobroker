# nobroker -- one command for everything.
#
#   make build   ->  dist/nobroker.pyz, the whole tool as one runnable file
#   make demo    ->  the end-to-end tour
#   make test    ->  122 tests, including crash recovery
#
# Every target delegates to make.py, which holds the actual task definitions, so
# the two can never drift. If you do not have `make` -- which is the default on
# Windows -- use make.py directly and nothing changes:
#
#   python make.py build
#
# That is deliberate. `make` is a separately installed tool, and this project's
# claim is that you need Python and nothing else. The Makefile exists because
# `make test` is what a reviewer's fingers type; make.py exists because it is
# what actually runs everywhere.

PYTHON ?= python3

.DEFAULT_GOAL := help
.PHONY: help build demo video-demo test test-quick bench bench-md check-deps deps-proof cli clean

help:  ## List every task
	@$(PYTHON) make.py

build:  ## Bundle the tool into one runnable file (dist/nobroker.pyz)
	@$(PYTHON) make.py build

demo:  ## Run the end-to-end tour (start here)
	@$(PYTHON) make.py demo

video-demo:  ## Run the paced demo built for screen recording
	@$(PYTHON) make.py video-demo

test:  ## Run the full test suite, including crash recovery
	@$(PYTHON) make.py test

test-quick:  ## Run everything except the slow exhaustive-truncation sweep
	@$(PYTHON) make.py test-quick

bench:  ## Measure throughput on this machine
	@$(PYTHON) make.py bench

bench-md:  ## Same, as a Markdown table for the README
	@$(PYTHON) make.py bench-md

check-deps:  ## Fail if anything outside the standard library is imported
	@$(PYTHON) make.py check-deps

deps-proof:  ## Regenerate deps-proof.txt
	@$(PYTHON) make.py deps-proof

cli:  ## Show the CLI help
	@$(PYTHON) make.py cli

clean:  ## Remove caches, scratch queues and build output
	@$(PYTHON) make.py clean
