def main():
  HDFS_URI = "hdfs://hdfs.domain.cc/folder"
  sc = SparkContext()
  rdd = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
  rdd.saveAsNewAPIHadoopFile(HDFS_URI + "/01", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
  rdd = sc.parallelize([("d", 4), ("e", 5), ("f", 6)])
  rdd.saveAsNewAPIHadoopFile(HDFS_URI + "/02", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
  folder = TwoDHDFSMap(sc, HDFS_URI)
  print("hdfsURI test: ", folder.hdfsURI == HDFS_URI)
  print("folder[\"01\"][\"a\"] test: ", folder["01"]["a"] == 1)
  print("\"01\" in folder test: ", "01" in folder)
  print("\"02\" in folder test: ", "02" in folder)
  print("folder[\"02\"][\"d\"] test: ", folder["02"]["d"] == 4)

if __name__ == "__main__":
  main()