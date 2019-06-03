# ============
# Main targets
# ============


# -------------
# Configuration
# -------------

$(eval venvpath     := .venv_util)
$(eval pip          := $(venvpath)/bin/pip)
$(eval python       := $(venvpath)/bin/python)
$(eval pytest       := $(venvpath)/bin/pytest)
$(eval bumpversion  := $(venvpath)/bin/bumpversion)
$(eval twine        := $(venvpath)/bin/twine)
$(eval sphinx       := $(venvpath)/bin/sphinx-build)

# Setup Python virtualenv
setup-virtualenv:
	@test -e $(python) || `command -v virtualenv` --python=python2 --no-site-packages $(venvpath)


# -------
# Release
# -------

# Release this piece of software
# Synopsis:
#   make release bump=minor  (major,minor,patch)
release: bumpversion push sdist pypi-upload


# ===============
# Utility targets
# ===============
bumpversion: install-releasetools
	@$(bumpversion) $(bump)

push:
	git push && git push --tags

sdist:
	@$(python) setup.py sdist

pypi-upload: install-releasetools
	twine upload --skip-existing --verbose dist/*.tar.gz

install-doctools: setup-virtualenv
	@$(pip) install --quiet --requirement requirements-docs.txt --upgrade

install-releasetools: setup-virtualenv
	@$(pip) install --quiet --requirement requirements-release.txt --upgrade
