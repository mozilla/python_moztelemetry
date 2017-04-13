.. python_moztelemetry documentation master file, created by
   sphinx-quickstart on Wed Jul 27 19:27:29 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

python_moztelemetry
===================

A simple library to fetch and analyze data collected by the Mozilla Telemetry service.
Objects collected by Telemetry are called ``pings``.
A ping has a number of properties (aka ``dimensions``) and a payload.
A session of Telemetry data analysis/manipulation typically starts with a query that filters the objects by one or more dimensions.
This query can be expressed using either an orm-like api, :ref:`Dataset` or a simple
function, :ref:`get_pings`.


.. toctree::
   :maxdepth: 2

   api




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

