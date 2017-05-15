"""
Original code from spark-hyperloglog

import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._

val hllMerge = new HyperLogLogMerge
sqlContext.udf.register("hll_merge", hllMerge)
sqlContext.udf.register("hll_create", hllCreate _)
sqlContext.udf.register("hll_cardinality", hllCardinality _)

val frame = sc.parallelize(List("a", "b", "c", "c")).toDF("id")
val count = frame
  .select(expr("hll_create(id, 12) as hll"))
  .groupBy()
  .agg(expr("hll_cardinality(hll_merge(hll)) as count"))
  .show()



The interface to the spark-hyperloglog API

from pyspark.sql.types import *
from moztelemetry import hll

hll.register(spark)

schema = StructType([StructField('id', StringType(), True)])
df = spark.createDataFrame([("a",), ("b",), ("c",), ("c",)], ['id'], schema)
count = df
    .select(hll.create('id', 12).alias('hll'))
    .agg(hll.cardinality(hll.merge('hll')).alias('count'))
    .show()

"""
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.types import BinaryType, LongType


def hyperloglog():
    session = SparkSession.builder.getOrCreate()
    return session.sparkContext._jvm.com.mozilla.spark.sql.hyperloglog


def register(session):
    session.catalog.registerFunction(
        'hll_merge', merge, returnType=BinaryType()
    )
    session.catalog.registerFunction(
        'hll_cardinality', cardinality, returnType=LongType()
    )
    session.catalog.registerFunction(
        'hll_create', create, returnType=BinaryType(),
    )


def merge(col):
    session = SparkSession.builder.getOrCreate()
    _hll_merge = hyperloglog().aggregates.HyperLogLogMerge().apply
    return Column(_hll_merge(_to_seq(session.sparkContext,
                  [col], _to_java_column)))


def cardinality(col):
    hllCardinality = hyperloglog().functions.package.hllCardinality
    return Column(hllCardinality(col))


def create(element, bits):
    hllCreate = hyperloglog().functions.package.hllCreate
    return Column(hllCreate(element, bits))
