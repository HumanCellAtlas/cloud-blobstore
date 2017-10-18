MODULES=cloud_blobstore tests

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports $(MODULES)

test_srcs := $(wildcard tests/test_*.py)

test: lint mypy $(test_srcs)
	coverage combine
	rm -f .coverage.*

$(test_srcs): %.py :
	coverage run -p --source=cloud_blobstore -m unittest $@

.PHONY: test lint mypy $(test_srcs)
