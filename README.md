# python_moztelemetry
Spark bindings for Mozilla Telemetry

## Updating the package on PyPI
- Create / update your `~/.pypirc`
```
[distutils]
index-servers=pypi
[pypi]
repository = https://pypi.python.org/pypi
[pypi]
username:example_user
password:example_pass
```
- Fetch the latest code with `git pull`
- Update PyPI with `python setup.py sdist upload`
