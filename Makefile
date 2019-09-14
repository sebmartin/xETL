VENV=.venv
PYVERSION=3.7.3

${VENV}:
	@which pyenv > /dev/null 2>&1 || (echo "Install pyenv (e.g. brew install pyenv)" && exit 1)
	pyenv local ${PYVERSION} || pyenv install ${PYVERSION} && pyenv local ${PYVERSION}
	virtualenv --version || pip install virtualenv
	virtualenv ${VENV}

${VENV}/bin/activate: ${VENV} requirements.txt
	. ${VENV}/bin/activate; pip install -r requirements.txt
	touch ${VENV}/bin/activate

test: ${VENV}
	. ${VENV}/bin/activate; py.test -vv tests

clean:
	find . -name __pycache__ | xargs rm -rf
	find . -name "*.pyc" | xargs rm -f

clean-env: clean
	rm -rf ${VENV}
