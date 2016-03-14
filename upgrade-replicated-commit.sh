#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.217 128.111.84.169 128.111.84.186 128.111.84.206 128.111.84.212 128.111.84.216 128.111.84.228 128.111.84.229 128.111.84.249)
currentPEMfileLocation="/Users/fmalinowski/Downloads/ReplicatedCommit.pem"
currentJarFileLoc="./replicated-commit/target/replicated-commit-1.0-SNAPSHOT-jar-with-dependencies.jar"

$(chmod 600 $currentPEMfileLocation)

for server in ${servers[@]}
do
	echo "Uploading replicated commit server app on ${server}"
	$(scp -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} ${currentJarFileLoc} root@${server}:~/replicated-commit.jar)
done
