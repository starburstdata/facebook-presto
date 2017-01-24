=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you if your cluster is facing a specific performance problem.


When setting up a cluster for a specific workload it may be necessary to adjust the
following properties to ensure optimal performance:

    * :ref:`distributed-joins-enabled<general-properties>`
    * :ref:`query.max-memory<query-properties>`
    * :ref:`query.max-memory-per-node<query-properties>`
    * :ref:`query.initial-hash-partitions<query-properties>`
    * :ref:`task.concurrency<task-properties>`

Those and other Presto properties are described in :doc:`properties section<properties>`.

As an example, on a 11-node cluster dedicated to Presto (1 Coordinator + 10 Workers) with 8-core CPU and 128GB of RAM
per node, you might want to start tuning with following values:

    * `query.max-memory = 200GB`
    * `query.max-memory-per-node = 32GB`
    * `query.initial-hash-partitions = 10`
    * `task.concurrency = 8`

A reasonable value for ``query.max-memory-per-node`` is to have it set to half of the JVM config max memory,
though if your workload is highly concurrent, you may want to use a lower value for ``query.max-memory-per-node``.
``query.max-memory`` could have go up to ````query.max-memory-per-node`` * ``number of workers``.
``query.initial-hash-partitions`` is set to the total number of workers in the cluster and ``task.concurrency`` to the
number of cores.

If this guide does not suit your needs, You may look for more tuning options on
:doc:`/admin/properties` page.

Config Properties
-----------------

See :doc:`/admin/properties`.

JVM Settings
------------

The following can be helpful for diagnosing GC issues:

.. code-block:: none

    -XX:+PrintGCApplicationConcurrentTime
    -XX:+PrintGCApplicationStoppedTime
    -XX:+PrintGCCause
    -XX:+PrintGCDateStamps
    -XX:+PrintGCTimeStamps
    -XX:+PrintGCDetails
    -XX:+PrintReferenceGC
    -XX:+PrintClassHistogramAfterFullGC
    -XX:+PrintClassHistogramBeforeFullGC
    -XX:PrintFLSStatistics=2
    -XX:+PrintAdaptiveSizePolicy
    -XX:+PrintSafepointStatistics
    -XX:PrintSafepointStatisticsCount=1
