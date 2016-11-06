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
	rm -rf dist selinon.egg-info build docs.source/api docs/build/

check:
	@# We have to adjust PYTHONPATH so we use our own Celery and modified Selinon Dispatcher for testing
	@# Make sure we have -p no:celery otherwise py.test is trying to do dirty stuff with loading celery.contrib
	PYTHONPATH="test/:${PYTHONPATH}" py.test --cov=./selinon -vvl --timeout=2 -p no:celery test/*.py

doc:
	@sphinx-apidoc -e -o docs.source/api selinon -f
	@make -f Makefile.docs html
	@echo "Documentation available at 'docs/index.html'"

docs: doc
html: doc

