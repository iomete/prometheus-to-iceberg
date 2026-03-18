CONFIG ?= config/metrics.yaml

.PHONY: package deps clean run

package: clean
	zip -r prometheus_to_iceberg.zip prometheus_to_iceberg/ -x '*__pycache__/*' '*.pyc'

deps: clean-deps
	pip install -t dependencies -r requirements.txt
	cd dependencies && zip -r ../dependencies.zip . -x '*__pycache__/*' '*.pyc' '*.dist-info/*'
	rm -rf dependencies

clean:
	rm -f prometheus_to_iceberg.zip

clean-all: clean clean-deps

clean-deps:
	rm -f dependencies.zip
	rm -rf dependencies

run:
	python job.py --config $(CONFIG)
