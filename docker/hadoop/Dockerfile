# The stock image has no data directories, so a named volume mounted at a path
# that does not exist arrives owned by root and the hadoop user cannot write to
# it. Creating them here means the volume inherits the right ownership.
ARG HADOOP_VERSION=3.4.1
FROM apache/hadoop:${HADOOP_VERSION}

USER root
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data \
 && chown -R hadoop:users /hadoop
USER hadoop
