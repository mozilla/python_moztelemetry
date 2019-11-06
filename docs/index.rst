.. python_moztelemetry documentation master file, created by
   sphinx-quickstart on Wed Jul 27 19:27:29 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

python_moztelemetry
===================

.. warning::
    This library will stop working *VERY SOON* (if it hasn't stopped
    working already)

    Please see `this post <https://mail.mozilla.org/pipermail/fx-data-dev/2019-November/000291.html>`_ to fx-data-dev for more information.

A simple library to fetch and analyze data collected by the Mozilla Telemetry service.
Objects collected by Telemetry are called ``pings``.
A ping has a number of properties (aka ``dimensions``) and a payload.

For example usage and additional context on how this library fits into the
broader landscape of Mozilla telemetry analysis tools, see
`moztelemetry on docs.telemetry.mozilla.org <https://docs.telemetry.mozilla.org/tools/spark.html#the-moztelemetry-library>`_.
To browse the source code, see https://github.com/mozilla/python_moztelemetry

A session of Telemetry data analysis/manipulation typically starts
with a :ref:`Dataset` query that filters the objects by one or more
dimensions, and then extracts the items of interest from their payload.

.. toctree::
   :maxdepth: 2

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

