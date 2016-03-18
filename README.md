# Replicated Commit and Replicated Log (Google Spanner) protocols 
[![](https://travis-ci.org/fmalinowski/replicated-commit.svg?branch=master)](https://travis-ci.org/fmalinowski/replicated-commit)

In this repository, we have implemented two geo-distributed protocols: Replicated Commit protocol (Hatem Mahmoud et al. Low-Latency Multi-Datacenter Databases using Replicated Commit. In VLDB, 2013) and the Replicated Log protocol used in Google Spanner (J.C. Corbett et al. Spanner: Google’s Globally-Distributed Database. In OSDI, 2012).

## Replicated Commit protocol implementation
Our implementation of the Replicated Commit protocol has been tested under a distributed system of 9 servers (3 datacenters containing each 3 shards). The source code for this implementation is in the replicated-commit subfolder. This folder contains only the source code for the server app that is run on a shard.
We use the YCSB+T benchmark client to generate transactions and we extended the DB class of the YCSB client to use our distributed architecture. The source code of this client app is in the client subfolder. The client class used by YCSB for our architecture is at this following path: client/ycsb/replicatedcommit/src/main/java/com/yahoo/ycsb/db/ReplicatedCommit.java


## Replicated Log protocol implementation
The spanner architecture has not been fully finished to support a distributed system environment like eucalyptus because of a lack of time. It can be run locally though. The server app lies in the spanner subfolder. The YCSB client version of spanner is present in the spanner-client subfolder.


## How to run the Replicated Commit server app?
You need to have Hbase v1.0.3 installed on the machine hosting your server app. (It needs to be installed on each shard as it is used to persist the data as a simple datastore).
You need to be in the ``replicated-commit`` subfolder. Then you need to compile the project (Apache Maven is required for this operation) by using the command ``mvn package``.
The jar file containing the whole compiled source code will be located in the subfolder target of that project with the following name: ``replicated-commit-1.0-SNAPSHOT-jar-with-dependencies.jar``.
Prior to running the server app, you need to place the config.properties file at the level as the jar file and change the value of the fields. You can customize the number of datacenters and shards per datacenter. You need also to select the IP addresses of each shard in each datacenter.
Once this is done, you can run the server app by typing the following command: ``java -DactivateLog=true -DactivateLatencySimu=true -cp replicated-commit-1.0-SNAPSHOT-jar-with-dependencies.jar edu.ucsb.rc.App``. You can activate a log of all actions done on the shard by turning on or off the java property ``-DactivateLog`` with the right boolean true or false. You can also turn on or off the simulation of delays between datacenters by setting the right jar property ``-DactivateLatencySimu=true`` with the right boolean.
The commit log of the successfully committed transactions will be available in a file called commitLog.txt


## How to run the Replicated Commit client app (YCSB+T)?
You need to go to the following subfolder: ``client/ycsb``. Then you need to build the package with Apache maven by typing the following command: ``mvn package``.
Then you can run YCSB as you would do normally and provide the DB adaptor used for Replicated commit. Here's an example: ``bin/ycsb load replicatedcommit -P workloads/workloada`` to load the data in the distributed architecture and then ``bin/ycsb run replicatedcommit -P workloads/workloada`` to run the benchmark.