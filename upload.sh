#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.160 128.111.84.169 128.111.84.186 128.111.84.206 128.111.84.212 128.111.84.216 128.111.84.228 128.111.84.229 128.111.84.249)
currentPEMfileLocation="/Users/fmalinowski/Downloads/ReplicatedCommit.pem"
currentJarFileLoc="./replicated-commit/target/replicated-commit-1.0-SNAPSHOT-jar-with-dependencies.jar"
currentHBASEconfigLoc="./hbase-site.xml"

$(chmod 600 $currentPEMfileLocation)

currentServer=0
tmpConfigFile="tmp-end.config"
finalTmpConfigFile="tmp.config"

rm -f $tmpConfigFile
rm -f $finalTmpConfigFile

echo "shardListeningPort = 50000" >> $tmpConfigFile
echo "clientListeningPort = 50001" >> $tmpConfigFile
echo "numberDatacenters = ${datacenters}" >> $tmpConfigFile
echo "numberShardsPerDatacenter = ${shards}" >> $tmpConfigFile

for server in ${servers[@]}
do
	currentShard=$((currentServer%shards))
	currentDatacenter=$(($currentServer / $shards))
	currentServer=$((currentServer + 1))
	
	shardIpLineForConfig="DC${currentDatacenter}-Shard${currentShard} = $server"
	echo $shardIpLineForConfig >> $tmpConfigFile
done

currentServer=0

wget http://cs.ucsb.edu/~fmalinowski/jdk-7u79-linux-i586.tar.gz
wget http://cs.ucsb.edu/~fmalinowski/hbase-1.0.3-bin.tar.gz

for server in ${servers[@]}
do
	currentShard=$((currentServer%shards))
	currentDatacenter=$(($currentServer / $shards))
	currentServer=$((currentServer + 1))
	
	rm -f $finalTmpConfigFile
	cp $tmpConfigFile $finalTmpConfigFile
	echo "currentDatacenter = ${currentDatacenter}" >> $finalTmpConfigFile
	echo "currentShard = ${currentShard}" >> $finalTmpConfigFile

	echo "Setting up the shard ${currentShard} of datacenter ${currentDatacenter}"
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'rm -f ~/hbase-1.0.3-bin.tar.gz; rm -rf ~/hbase-1.0.3; rm -f ~/config.properties; rm -f replicated-commit.jar; cd /opt/; rm -rf jdk-7u79-linux-i586*;')

	echo "Installing Java 1.7 and HBase - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} jdk-7u79-linux-i586.tar.gz root@${server}:/opt/)
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} hbase-1.0.3-bin.tar.gz root@${server}:~/)
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'cd /opt/; tar xzf jdk-7u79-linux-i586.tar.gz; export JAVA_HOME=/opt/jdk1.7.0_79; export PATH=$PATH:/opt/jdk1.7.0_79/bin:/opt/jdk1.7.0_79/jre/bin; export JRE_HOME=/opt/jdk1.7.0_79/jre; cd ~/; tar xfz hbase-1.0.3-bin.tar.gz;')

	echo "Upload replicated commit app - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${currentJarFileLoc} root@${server}:~/replicated-commit.jar)
	echo "Upload config file - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${finalTmpConfigFile} root@${server}:~/config.properties)
	echo "Upload hbase config file - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${currentHBASEconfigLoc} root@${server}:~/hbase-1.0.3/conf/hbase-site.xml)
done

rm -f $tmpConfigFile
rm -f $finalTmpConfigFile

rm -f jdk-7u79-linux-i586.tar.gz
rm -f hbase-1.0.3-bin.tar.gz
