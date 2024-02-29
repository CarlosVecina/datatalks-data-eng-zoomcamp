.PHONY: all install-java install-spark

SSH_ZONE := --zone "us-west2-a"
SSH_INSTANCE := "my-instance"
SSH_PROJECT := --project "concise-quarter-411516"
SSH_OPTIONS := $(SSH_ZONE) $(SSH_INSTANCE) $(SSH_PROJECT)
SSH_COMMAND := gcloud compute ssh $(SSH_OPTIONS) --command

all: install-java install-spark

install-java:
	$(SSH_COMMAND) 'wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz && \
		tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz && \
		export JAVA_HOME="${HOME}/jdk-11.0.2" && \
		export PATH="${JAVA_HOME}/bin:${PATH}" && \
		java --version && \
		rm openjdk-11.0.2_linux-x64_bin.tar.gz'

install-spark:
	$(SSH_COMMAND) 'wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
		tar xzfv spark-3.3.2-bin-hadoop3.tgz && \
		export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3" && \
		export PATH="${SPARK_HOME}/bin:${PATH}" && \
		rm spark-3.3.2-bin-hadoop3.tgz'
