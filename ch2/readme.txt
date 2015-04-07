flume-logs
==========

Apache Flume and Hive to process log files on a Hadoop cluster.

This repository provides a working project to show how to ingest simple log files into Hadoop with Flume, and then to make the information in the log files available for easy access and processing with Hive.  

There is an accompanying article at [Log Files with Flume and Hive](http://www.lopakalogic.com/articles/hadoop-articles/log-files-flume-hive/) that provides additional details and thoughts on how the solution was put together and why certain decisions were made.  

Hope you find this useful!

./gen_events.py 2>&1 | nc 127.0.0.1 9999