#!/bin/bash                                                                                                                                                                    

datacenters=3
shards=3
servers=(128.111.84.194 128.111.84.193 128.111.84.205 128.111.84.223 128.111.84.229 128.111.84.206 128.111.84.233 128.111.84.212 128.111.84.217)
currentPEMfileLocation="/Users/fmalinowski/Downloads/FrancoisMalinowski.pem"

$(chmod 600 $currentPEMfileLocation)

for server in ${servers[@]}
do
	$(ssh -o "StrictHostKeyChecking no" -i ${currentPEMfileLocation} root@${server} 'kill -9 `cat save_pid.txt`; ./hbase-0.94.27/bin/stop-hbase.sh;')
done
