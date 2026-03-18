CONFIG ?= config/metrics.yaml

.PHONY: package clean run

package: clean
	zip -r prometheus_to_iceberg.zip prometheus_to_iceberg/ -x '*__pycache__/*' '*.pyc'

clean:
	rm -f prometheus_to_iceberg.zip

run:
	python job.py --config $(CONFIG)
