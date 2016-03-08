#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.194 128.111.84.193 128.111.84.205 128.111.84.223 128.111.84.229 128.111.84.206 128.111.84.233 128.111.84.212 128.111.84.217)
currentPEMfileLocation="/Users/fmalinowski/Downloads/FrancoisMalinowski.pem"

$(chmod 600 $currentPEMfileLocation)

for server in ${servers[@]}
do
	echo "Connect to server ${server} - Stop HBase and restart it, Start replicated commit app."
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; ./hbase-0.94.27/bin/stop-hbase.sh; ./hbase-0.94.27/bin/start-hbase.sh; echo "disable \"usertable\""; ./hbase-0.94.27/bin/hbase shell; echo "drop \"usertable\""; ./hbase-0.94.27/bin/hbase shell; echo "create \"usertable\", \"cf\""; ./hbase-0.94.27/bin/hbase shell; nohup java -cp replicated-commit.jar edu.ucsb.rc.App > my.log 2>&1& echo $! > save_pid.txt;')
done
