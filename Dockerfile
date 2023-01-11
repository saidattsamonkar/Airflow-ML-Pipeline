FROM quay.io/astronomer/astro-runtime:7.1.0
RUN export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"
