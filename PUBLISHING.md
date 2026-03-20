# Publishing s4db to PyPI

This document describes how to build and publish `s4db` to PyPI using `twine`.

## Prerequisites

Install the required tools:

```bash
pip install build twine
```

You will also need a [PyPI account](https://pypi.org/account/register/) and an API token. To
generate a token, go to Account Settings > API tokens on PyPI.

Store the token in `~/.pypirc` so you do not have to pass it on every upload:

```ini
[pypi]
  username = __token__
  password = pypi-<your-token-here>
```

## Build

From the repository root, run:

```bash
python3 -m build
```

This produces two artifacts in `dist/`:

- `s4db-<version>.tar.gz` - source distribution
- `s4db-<version>-py3-none-any.whl` - wheel

## Check the Distribution

Verify the package metadata before uploading:

```bash
python3 -m twine check dist/*
```

Fix any warnings or errors before proceeding.

## Upload to TestPyPI (Recommended First)

Test the upload against [TestPyPI](https://test.pypi.org) before publishing to production:

```bash
python3 -m twine upload --repository testpypi dist/*
```

Verify the package installs correctly from TestPyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ s4db
```

## Upload to PyPI

Once verified, upload to production PyPI:

```bash
python3 -m twine upload dist/*
```

The package will be available at `https://pypi.org/project/s4db/` within a few minutes.

## Versioning

Before each release, update the `version` field in `pyproject.toml`. PyPI does not allow
re-uploading a file for an existing version.

```toml
[project]
version = "0.2.0"
```
