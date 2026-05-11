# SAM build target for the cv-dispatch Lambda.
#
# `sam build` looks for a Makefile in CodeUri when the function declares
# `Metadata: BuildMethod: makefile`. The required target name follows the
# convention `build-<LogicalResourceId>`.
#
# We use this to bundle the two shared modules from the repo root
# (`cv_batch_dispatch.py` and `cv_metrics.py`) into the Lambda zip
# without permanently duplicating them under `lambda/cv_dispatch/`.

REPO_ROOT := $(realpath $(CURDIR)/../..)

.PHONY: build-CvDispatchFunction

build-CvDispatchFunction:
	@echo "==> Bundling cv-dispatch Lambda"
	@echo "    repo_root  = $(REPO_ROOT)"
	@echo "    artifacts  = $(ARTIFACTS_DIR)"

	# Shared modules from repo root — these come along with handler.py
	# but live outside lambda/cv_dispatch/ to stay deduplicated.
	cp $(REPO_ROOT)/cv_batch_dispatch.py $(ARTIFACTS_DIR)/
	cp $(REPO_ROOT)/cv_metrics.py        $(ARTIFACTS_DIR)/

	# Function code.
	cp handler.py $(ARTIFACTS_DIR)/

	# Pip deps.
	pip install --quiet -r requirements.txt -t $(ARTIFACTS_DIR)

	@echo "==> Bundle contents:"
	@ls -la $(ARTIFACTS_DIR) | head -30
