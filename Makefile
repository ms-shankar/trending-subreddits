.PHONY: clean setup test

all: clean setup test

clean:
	rm -rf .virtualenv/
	rm -rf .pytest_cache/

setup:
	virtualenv --python=python3.7 .virtualenv
	( \
		. .virtualenv/bin/activate; \
		pip3 install --upgrade pip; \
		pip3 install -r requirements.txt; \
		pip3 install --upgrade . ; \
  )

test:
	( \
		. .virtualenv/bin/activate; \
		python -m unittest -v \
  )



