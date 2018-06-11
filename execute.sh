#!/bin/bash
/opt/apache/spark-1.6.1-bin-hadoop2.6/bin/spark-submit \
--master spark://leepong1:7070 \
--class com.spark.streaming.WordCount \
--deploy-mode client \
/opt/datas/out/artifacts/streaming1/unnamed.jar