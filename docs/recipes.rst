Recipes
=======

Benchmarking an existing cluster
--------------------------------

.. warning::

    If you are just getting started with Rally and don't understand how it works, please do NOT run it against any production or production-like cluster. Besides, benchmarks should be executed in a dedicated environment anyway where no additional traffic skews results.

.. note::

    We assume in this recipe, that Rally is already properly :doc:`configured </configuration>`.

Consider the following configuration: You have an existing benchmarking cluster, that consists of three Elasticsearch nodes running on ``10.5.5.10``, ``10.5.5.11``, ``10.5.5.12``. You've setup the cluster yourself and want to benchmark it with Rally. Rally is installed on ``10.5.5.5``.

.. image:: benchmark_existing.png
   :alt: Sample Benchmarking Scenario

First of all, we need to decide on a track. So, we run ``esrally list tracks``::

    Name        Description                                                           Challenges
    ----------  --------------------------------------------------------------------  -----------------------------------------------------------------------------------------------------------------------------------------------
    geonames    Standard benchmark in Rally (8.6M POIs from Geonames)                 append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts
    geopoint    60.8M POIs from PlanetOSM                                             append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts
    logging     Logging benchmark                                                     append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts
    nyc_taxis   Trip records completed in yellow and green taxis in New York in 2015  append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica
    percolator  Percolator benchmark based on 2M AOL queries                          append-no-conflicts
    pmc         Full text benchmark containing 574.199 papers from PMC                append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts

We're interested in a full text benchmark, so we'll choose to run ``pmc``. If you have your own data that you want to use for benchmarks, then please :doc:`create your own track</adding_tracks>` instead; the metrics you'll gather which be representative and much more useful than some default track.

Next, we need to know which machines to target which is easy as we can see that from the diagram above.

Finally we need to check which :doc:`pipeline </pipelines>` to use. For this case, the ``benchmark-only`` pipeline is suitable as we don't want Rally to provision the cluster for us.

Now we can invoke Rally::

    esrally --track=pmc --target-hosts=10.5.5.10:9200,10.5.5.11:9200,10.5.5.12:9200 --pipeline=benchmark-only

If you have `X-Pack Security <https://www.elastic.co/products/x-pack/security>`_  enabled, then you'll also need to specify another parameter to use https and to pass credentials::

    esrally --track=pmc --target-hosts=10.5.5.10:9243,10.5.5.11:9243,10.5.5.12:9243 --pipeline=benchmark-only --client-options="basic_auth_user:'elastic',basic_auth_password:'changeme'"


Benchmarking a remote cluster
-----------------------------

Contrary to the previous recipe, you want Rally to provision all cluster nodes.

.. note::

   At the moment, Rally can only provision a single remote node but support for provisioning of multi-node clusters is a high priority topic on our roadmap.

We will use the following configuration for the example:

* You will start Rally on ``10.5.5.5``. We will call this machine the "benchmark coordinator".
* Your Elasticsearch cluster will run on ``10.5.5.10``. We will call this machine the "benchmark candidate".

.. image:: benchmark_existing.png
   :alt: Sample Benchmarking Scenario

To run a benchmark for this scenario follow these steps:

1. :doc:`Install </install>` and :doc:`configure </configuration>` Rally on all machines. Be sure that the same version is installed on all of them.
2. Start the Rally daemon on each machine. The Rally daemon allows Rally to communicate with all remote machines. On the benchmark coordinator run ``esrallyd start --node-ip=10.5.5.5 --coordinator-ip=10.5.5.5`` and on the benchmark candidate run ``esrallyd start --node-ip=10.5.5.10 --coordinator-ip=10.5.5.5``. The ``--node-ip`` parameter tells Rally the IP of the machine on which it is running. As some machines have more than one network interface, Rally will not attempt to auto-detect the machine IP. The ``--coordinator-ip`` parameter tells Rally the IP of the benchmark coordinator node.
3. Start the benchmark by invoking Rally as usual on the benchmark coordinator, for example: ``esrally --distribution-version=5.0.0 --target-hosts=10.5.5.10:9200``. Rally will derive from the ``--target-hosts``  parameter that it should provision the node ``10.5.5.10``.
4. After the benchmark has finished you can stop the Rally daemon again. On the benchmark coordinator and on the benchmark candidate run ``esrallyd stop``.

.. note::

   Logs are managed per machine, so all relevant log files and also telemetry output is stored on the benchmark candidate but not on the benchmark coordinator.

Now you might ask yourself what the differences to benchmarks of existing clusters are. In general you should aim to give Rally as much control as possible as benchmark are easier reproducible and you get more metrics. The following table provides some guidance on when to choose

===================================================== ========================================================================
Your requirement                                      Recommendation
===================================================== ========================================================================
You want to use Rally's telemetry devices             Use Rally daemon, as it can provision the remote node for you
You want to benchmark a source build of Elasticsearch Use Rally daemon, as it can build Elasticsearch for you
You want to tweak the cluster configuration yourself  Set up the cluster by yourself and use ``--pipeline=benchmark-only``
You need to run a benchmark with plugins              Set up the cluster by yourself and use ``--pipeline=benchmark-only``
You need to run a benchmark against multiple nodes    Set up the cluster by yourself and use ``--pipeline=benchmark-only``
===================================================== ========================================================================

Rally daemon will be able to cover most of the cases described above in the future so there should be almost no case where you need to use the ``benchmark-only`` pipeline.
