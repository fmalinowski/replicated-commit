#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.160 128.111.84.169 128.111.84.186 128.111.84.206 128.111.84.212 128.111.84.216 128.111.84.228 128.111.84.229 128.111.84.249)
currentPEMfileLocation="/Users/fmalinowski/Downloads/ReplicatedCommit.pem"

$(chmod 600 $currentPEMfileLocation)

for server in ${servers[@]}
do
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; export PATH=$PATH:/opt/jdk1.7.0_79/bin:/opt/jdk1.7.0_79/jre/bin; export JRE_HOME=/opt/jdk1.7.0_79/jre; kill -9 `cat save_pid.txt`; ./hbase-1.0.3/bin/stop-hbase.sh > /dev/null;')
done
