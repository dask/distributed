FROM mdurant/docker_images:hadoop

ADD docker-files/start.sh /opt/hadoop/run.sh


# Build:
#
# $ docker build -t distributed-hdfs-test .

# Configure host to access container HDFS:
#
# $ sudo bash -c 'echo "127.0.0.1 hdfs-container" >> /etc/hosts'

# Execution:
#
# $ rm -f hdfs-initialized
# $ docker run -d -h hdfs-container -v$(pwd):/host -p8020:8020 -p 50070:50070 distributed-hdfs-test
