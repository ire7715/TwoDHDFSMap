from pyspark import SparkContext

class TwoDHDFSMap(object):
  def __init__(self, sc, hdfsURI, \
    inputFormatClass="org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat", \
    outputFormatClass="org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat", \
    keyClass="org.apache.hadoop.io.Text", valueClass="org.apache.hadoop.io.IntWritable", \
    outURI=None):
    self.__hdfsURI = hdfsURI
    self.__sc = sc
    self.__map = dict()
    self.__ioOptions = dict()
    self.__ioOptions["inputFormatClass"] = inputFormatClass
    self.__ioOptions["outputFormatClass"] = outputFormatClass
    self.__ioOptions["keyClass"] = keyClass
    self.__ioOptions["valueClass"] = valueClass
    self.__outURI = outURI

  @property
  def hdfsURI(self):
      return self.__hdfsURI

  def __getitem__(self, key):
    if key not in self.__map:
      try:
        rdd = self.__sc.newAPIHadoopFile(self.__hdfsURI + "/" + key, \
          self.__ioOptions["inputFormatClass"], \
          self.__ioOptions["keyClass"], self.__ioOptions["valueClass"])
        self.__map[key] = rdd.collectAsMap()
      except:
        # no such an index
        return None
    return self.__map[key]

  def __setitem__(self, key, value):
    self.__map[key] = value
    return value

  def __contains__(self, key):
    self[key]
    return key in self.__map

  def __del__(self):
    if self.__outURI:
      for key, value in self.__map.iteritems():
        rdd = self.__sc.parallelize(value.items())
        rdd.saveAsNewAPIHadoopFile(self.__outURI + "/" + key, \
          self.__ioOptions["outputFormatClass"], \
          self.__ioOptions["keyClass"], self.__ioOptions["valueClass"])

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
