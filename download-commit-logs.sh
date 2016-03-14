#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.217 128.111.84.169 128.111.84.186 128.111.84.206 128.111.84.212 128.111.84.216 128.111.84.228 128.111.84.229 128.111.84.249)
currentPEMfileLocation="/Users/fmalinowski/Downloads/ReplicatedCommit.pem"

$(chmod 600 $currentPEMfileLocation)
currentServer=0

mkdir commitLogs

for server in ${servers[@]}
do
	currentShard=$((currentServer%shards))
	currentDatacenter=$(($currentServer / $shards))
	currentServer=$((currentServer + 1))

	commitLogFileName="DC${currentDatacenter}-Shard${currentShard}-commitLog.txt"

	echo "Downloading commit log from ${server}."
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server}:~/commitLog.txt  commitLogs/${commitLogFileName})
done