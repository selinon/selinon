TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv check doc docs html

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
	find . -name '*.pyc' -or -name '__pycache__' -print0 | xargs -0 rm -rf
	rm -rf venv
	rm -rf dist celeriac.egg-info build docs/source/api docs/build/

check:
	@# We have to adjust CWD so we use our own Celery and modified Celeriac Dispatcher for testing
	@python3 --version
	@cd test && python3 -m unittest -v test_systemState test_nodeFailures test_storage test_nowait test_flow

doc:
	@sphinx-apidoc -e -o docs/source/api celeriac -f
	@make -C docs html
	@echo "Documentation available at 'docs/build/html/index.html'"

docs: doc
html: doc

