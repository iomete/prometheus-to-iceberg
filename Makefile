CONFIG ?= config/metrics.yaml

.PHONY: package clean run

package: clean
	cd prometheus_to_iceberg && zip -r ../prometheus_to_iceberg.zip . -x '__pycache__/*' '*.pyc'

clean:
	rm -f prometheus_to_iceberg.zip

run:
	python job.py --config $(CONFIG)
