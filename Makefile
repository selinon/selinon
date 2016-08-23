TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv check

install:
	sh ./bump-version.sh
	pip3 install -r requirements.txt
	python3 setup.py install

uninstall:
	python3 setup.py install --record ${TEMPFILE} && \
		cat ${TEMPFILE} | xargs rm -rf && \
		rm -f ${TEMPFILE}

venv:
	virtualenv -p python3 venv && source venv/bin/activate && pip3 install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

clean:
	find . -name '*.pyc' -or -name '__pycache__' | xargs rm -rf
	rm -rf venv
	rm -rf dist celeriac.egg-info build

check:
	@# We have to adjust CWD so we use our own Celery and modified Celeriac Dispatcher for testing
	cd test && python3 -m unittest -v test_systemState test_nodeFailures test_storage test_nowait
