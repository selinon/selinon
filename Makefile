TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv check doc docs html api

install:
	sh ./bump-version.sh
	pip3 install -r requirements.txt
	python3 setup.py install

uninstall:
	python3 setup.py install --record ${TEMPFILE} && \
		cat ${TEMPFILE} | xargs rm -rf && \
		rm -f ${TEMPFILE}

devenv:
	@echo "Installing latest development requirements"
	pip3 install -U -r dev_requirements.txt
	@echo "Installing the latest Selinonlib sources from GitHub repo"
	pip3 install -U --force-reinstall git+https://github.com/selinon/selinonlib@master

venv:
	virtualenv venv && source venv/bin/activate && pip3 install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

clean:
	find . -name '*.pyc' -or -name '__pycache__' -print0 | xargs -0 rm -rf
	rm -rf venv coverage.xml
	rm -rf dist selinon.egg-info build docs/

pytest:
	@# We have to adjust PYTHONPATH so we use our own Celery and modified Selinon Dispatcher for testing
	@# Make sure we have -p no:celery otherwise py.test is trying to do dirty stuff with loading celery.contrib
	@echo ">>> Executing testsuite"
	PYTHONPATH="test/:${PYTHONPATH}" python3 -m pytest -s --cov=./selinon -vvl --nocapturelog --timeout=2 -p no:celery test/*.py

pylint:
	@echo ">>> Running pylint"
	pylint selinon

coala:
	@# We need to run coala in a virtual env due to dependency issues
	@echo ">>> Coala disabled"
	@#@echo ">>> Running coala"
	@#coala --non-interactive

pydocstyle:
	@echo ">>> Running pydocstyle"
	pydocstyle --match='(?!test_|version).*\.py' selinon

check: pytest pylint pydocstyle coala

api:
	@sphinx-apidoc -e -o docs.source/selinon/doc/ selinon -f

doc: api
	@make -f Makefile.docs html
	@echo "Documentation available at 'docs/index.html'"

docs: doc
html: doc
test: check
