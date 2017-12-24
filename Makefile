TEMPFILE := $(shell mktemp -u)


.PHONY: install
install:
	sh ./bump-version.sh
	pip3 install -r requirements.txt
	python3 setup.py install

.PHONY: uninstall
uninstall:
	python3 setup.py install --record ${TEMPFILE} && \
		cat ${TEMPFILE} | xargs rm -rf && \
		rm -f ${TEMPFILE}

.PHONY: devenv
devenv:
	@echo "Installing latest development requirements"
	pip3 install -U -r dev_requirements.txt

venv:
	virtualenv -p python3 venv && . venv/bin/activate && pip3 install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

coala-venv:
	@echo ">>> Preparing virtual environment for coala"
	@# We need to run coala in a virtual env due to dependency issues
	virtualenv -p python3 venv-coala
	. venv-coala/bin/activate && pip3 install -r coala_requirements.txt

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
	PYTHONPATH="test/:${PYTHONPATH}" python3 -m pytest -s --cov=./selinon -vvl --timeout=2 -p no:celery test/

.PHONY: pylint
pylint:
	@echo ">>> Running pylint"
	pylint selinon

.PHONY: coala
coala: coala-venv
	@echo ">>> Running coala"
	. venv-coala/bin/activate && coala --non-interactive

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
