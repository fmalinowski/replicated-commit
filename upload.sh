#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.181 128.111.84.239 128.111.84.214 128.111.84.175 128.111.84.231 128.111.84.194 128.111.84.205 128.111.84.211 128.111.84.209)
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
	echo "Installing Java 1.7 and HBase - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'rm -f ~/hbase-1.0.3-bin.tar.gz; rm -rf ~/hbase-1.0.3; rm -f ~/config.properties; rm -f replicated-commit.jar; cd /opt/; rm -rf jdk-7u79-linux-i586*; wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/7u79-b15/jdk-7u79-linux-i586.tar.gz"; tar xzf jdk-7u79-linux-i586.tar.gz; export JAVA_HOME=/opt/jdk1.7.0_79; export PATH=$PATH:/opt/jdk1.7.0_79/bin:/opt/jdk1.7.0_79/jre/bin; export JRE_HOME=/opt/jdk1.7.0_79/jre; cd ~/; wget http://apache.arvixe.com/hbase/hbase-1.0.3/hbase-1.0.3-bin.tar.gz; tar xfz hbase-1.0.3-bin.tar.gz; echo "127.0.0.1  'hostname'" > /etc/hosts;')
	echo "Upload replicated commit app - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${currentJarFileLoc} root@${server}:~/replicated-commit.jar)
	echo "Upload config file - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${finalTmpConfigFile} root@${server}:~/config.properties)
	echo "Upload hbase config file - shard ${currentShard} of datacenter ${currentDatacenter}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${currentHBASEconfigLoc} root@${server}:~/hbase-1.0.3/conf/hbase-site.xml)
done

rm -f $tmpConfigFile
rm -f $finalTmpConfigFile
