API
===

.. _dataset:

Dataset
-------

.. automodule:: moztelemetry.dataset
    :members:

.. _get_pings:

get_pings() (deprecated)
------------------------

.. autofunction:: moztelemetry.spark.get_pings

Using Spark RDDs
----------------
Both ``Dataset`` and ``get_pings`` return the data as a `Spark RDD <http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds>`_.
Users can then use the `RDD api <http://spark.apache.org/docs/latest/programming-guide.html#rdd-operations>`_ to further shape or transform the dataset.


Experimental APIs
-----------------

.. autofunction:: moztelemetry.zeppelin.show

.. automodule:: moztelemetry.hbase
    :members:
