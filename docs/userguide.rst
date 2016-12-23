Moztelemetry
============

A simple library to fetch and analyze data collected by the Mozilla Telemetry service.
Objects collected by Telemetry are called ``pings``.
A ping has a number of properties (aka ``dimensions``) and a payload.
A session of Telemetry data analysis/manipulation typically starts with a query that filters the objects by one or more dimensions.
This query can be expressed using either an orm-like api, ``Dataset``,  or a simple
function, ``get_pings`` (deprecated)


Dataset API
-----------

.. automodule:: moztelemetry.dataset
    :members:


get_pings() (deprecated)
------------------------

.. autofunction:: moztelemetry.spark.get_pings

Using Spark RDDs
----------------
Both ``Dataset`` and ``get_pings`` return the data as a `Spark RDD <http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds>`_.
Users can then use the `RDD api <http://spark.apache.org/docs/latest/programming-guide.html#rdd-operations>`_ to further shape or transform the dataset.


Experimental APIs
-----------------

HBaseMainSummaryView API
------------------------

.. automodule:: moztelemetry.hbase
    :members:
