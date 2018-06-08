API
===

.. _dataset:

Dataset
-------

.. automodule:: moztelemetry.dataset
    :members:

Deprecated ping methods
-----------------------

Before the Dataset API was available, a number of custom methods were
written for selecting a set of telemetry pings and extracting data
from them. These methods are somewhat convoluted and difficult to
understand, and are not recommended for new code.

.. autofunction:: moztelemetry.spark.get_pings
.. autofunction:: moztelemetry.spark.get_pings_properties
.. autofunction:: moztelemetry.spark.get_one_ping_per_client


Using Spark RDDs
----------------
Both ``Dataset`` and ``get_pings`` return the data as a `Spark RDD <http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds>`_.
Users can then use the `RDD api <http://spark.apache.org/docs/latest/programming-guide.html#rdd-operations>`_ to further shape or transform the dataset.


Experimental APIs
-----------------

.. autofunction:: moztelemetry.zeppelin.show

    :members:
