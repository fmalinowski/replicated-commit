#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.181 128.111.84.239 128.111.84.214 128.111.84.175 128.111.84.231 128.111.84.194 128.111.84.205 128.111.84.211 128.111.84.209)
currentPEMfileLocation="/Users/fmalinowski/Downloads/ReplicatedCommit.pem"

$(chmod 600 $currentPEMfileLocation)

for server in ${servers[@]}
do
	echo "Connect to server ${server} - Stop HBase and restart it, Start replicated commit app."
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; export PATH=$PATH:/opt/jdk1.7.0_79/bin:/opt/jdk1.7.0_79/jre/bin; export JRE_HOME=/opt/jdk1.7.0_79/jre; ./hbase-1.0.3/bin/start-hbase.sh > /dev/null;')
	# $(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; echo "disable \"usertable\"" | ./hbase-1.0.3/bin/hbase shell;')
	# $(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; echo "drop \"usertable\"" | ./hbase-1.0.3/bin/hbase shell;')
	# $(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; echo "create \"usertable\", \"cf\"" | ./hbase-1.0.3/bin/hbase shell;')
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'export JAVA_HOME=/opt/jdk1.7.0_79; export PATH=$PATH:/opt/jdk1.7.0_79/bin:/opt/jdk1.7.0_79/jre/bin; export JRE_HOME=/opt/jdk1.7.0_79/jre; nohup java -cp replicated-commit.jar edu.ucsb.rc.App > my.log 2>&1& echo $! > save_pid.txt;')
done
