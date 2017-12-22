TEMPFILE := $(shell mktemp -u)


.PHONY: install
install:
	sh ./bump-version.sh
	pip install -r requirements.txt
	python setup.py install

.PHONY: uninstall
uninstall:
	python setup.py install --record ${TEMPFILE} && \
		cat ${TEMPFILE} | xargs rm -rf && \
		rm -f ${TEMPFILE}

.PHONY: devenv
devenv:
	@echo "Installing latest development requirements"
	pip install -U -r dev_requirements.txt

.PHONY: venv
venv:
	virtualenv -p python venv && source venv/bin/activate && pip install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

.PHONY: clean
clean:
	find . -name '*.pyc' -or -name '__pycache__' -or -name '*.py.orig' | xargs rm -rf
	rm -rf venv venv-coala coverage.xml
	rm -rf dist selinon.egg-info build docs/

.PHONY: pytest
pytest:
	@# We have to adjust PYTHONPATH so we use our own Celery and modified Selinon Dispatcher for testing
	@# Make sure we have -p no:celery otherwise py.test is trying to do dirty stuff with loading celery.contrib
	@echo ">>> Executing testsuite"
	PYTHONPATH="test/:${PYTHONPATH}" python -m pytest -s --cov=./selinon -vvl --timeout=2 -p no:celery test/

.PHONY: pylint
pylint:
	@echo ">>> Running pylint"
	pylint selinon selinon-cli

.PHONY: coala
coala:
	@# We need to run coala in a virtual env due to dependency issues
	@echo ">>> Preparing virtual environment for coala" &&\
	  # setuptools is pinned due to dependency conflict &&\
	  [ -d venv-coala ] || virtualenv -p python venv-coala && . venv-coala/bin/activate && pip install coala-bears "setuptools>=17.0" &&\
	  echo ">>> Running coala" &&\
	  venv-coala/bin/python venv-coala/bin/coala --non-interactive

.PHONY: pydocstyle
pydocstyle:
	@echo ">>> Running pydocstyle"
	pydocstyle --match='(?!test_|version|codename|celery).*\.py' --match-dir='(?!predicates)' selinon

.PHONY: check
check: pytest pylint pydocstyle coala

.PHONY: api
api:
	@sphinx-apidoc -e -o docs.source/selinon/doc/ selinon -f

.PHONY: doc
doc: api
	@make -f Makefile.docs html
	@echo "Documentation available at 'docs/index.html'"

.PHONY: docs html test
docs: doc
html: doc
test: check
