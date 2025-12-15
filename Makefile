PYTHON ?= python

.PHONY: enums
enums:
	$(PYTHON) scripts/gen_enums.py
