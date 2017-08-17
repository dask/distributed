#!bin/bash

set -e

service ssh restart
ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts
ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts
echo 'export JAVA_HOME='$JAVA_HOME | cat - /opt/hadoop/etc/hadoop/hadoop-env.sh > temp
rm /opt/hadoop/etc/hadoop/hadoop-env.sh
mv temp /opt/hadoop/etc/hadoop/hadoop-env.sh
chmod 0600 ~/.ssh/authorized_keys
start-dfs.sh

touch /host/hdfs-initialized
sleep infinity
