# TwoDHDFSMap
A library for hdfs 2d dictionary with PySpark  
This project is dealing 2-dimensional dictionary, while you don't want to read the whole dictionary file. It automatically separate files by the key, and apply the lazy load strategy.

## Document

#### __init__  
&nbsp;parameters:  
&nbsp;&nbsp;*sc*: **required** SparkContext  
&nbsp;&nbsp;*hdfsURI*:**required** URI to HDFS file  
&nbsp;&nbsp;*inputFormatClass*=`"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"`  
&nbsp;&nbsp;*outputFormatClass*=`"org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"`  
&nbsp;&nbsp;*keyClass*=`"org.apache.hadoop.io.Text"`  
&nbsp;&nbsp;*valueClass*=`"org.apache.hadoop.io.IntWritable"`  
&nbsp;&nbsp;outURI=None: the output file name. If it's `None`, it would not generate an output file. Otherwise, it would generate a output file at given URI when destruction.

supports get(m[k]), set(m[k] = v), in(k in m) operators
