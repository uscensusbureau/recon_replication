# This should be sourced to run on VDI

PYSPARK_HOME=/usr/lib/spark/python

export PYTHONPATH=${PYTHONPATH}:${PYSPARK_HOME}:${PYSPARK_HOME}/lib/py4j-src.zip

