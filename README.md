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


## Updating histogram_tools.py
moztelemetry/histogram_tools.py is a mirror of its counterpart from
[mozilla-central](https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/histogram_tools.py).
To update it to the latest version you can run
```bash
bin/update_histogram_tools
```
