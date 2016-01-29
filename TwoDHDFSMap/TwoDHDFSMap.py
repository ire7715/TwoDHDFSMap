from hdfs import Config

class TwoDHDFSMap(object):
  def __init__(self, sc, hdfsURI=None, \
    inputFormatClass="org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat", \
    outputFormatClass="org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat", \
    keyClass="org.apache.hadoop.io.Text", valueClass="org.apache.hadoop.io.IntWritable", \
    outURI=None, hdfsConfigAlias=None):
    self.__hdfsURI = str(hdfsURI) if hdfsURI else None
    self.__sc = sc
    self.__map = dict()
    self.__ioOptions = dict()
    self.__ioOptions["inputFormatClass"] = str(inputFormatClass)
    self.__ioOptions["outputFormatClass"] = str(outputFormatClass)
    self.__ioOptions["keyClass"] = str(keyClass)
    self.__ioOptions["valueClass"] = str(valueClass)
    self.__outURI = str(outURI) if outURI else None

    if self.__hdfsURI:
      self.__client = Config.get_client(hdfsConfigAlias)
      tempKeys = self.__client.list(self.__hdfsURI + "/") # raise hdfs.util.HdfsError when directory doesn't exist
      self.__existingKeys = map(lambda direct: direct.split("/")[-1], tempKeys)
    else:
      self.__existingKeys = None

  @property
  def hdfsURI(self):
      return self.__hdfsURI

  def __getitem__(self, key):
    if "/" in key:
      raise KeyError("A slash(/) in the key(\"" + key + "\") is not allowed.")
    if key not in self.__map:
      if self.__hdfsURI and key in self.__existingKeys:
        try:
          rdd = self.__sc.newAPIHadoopFile(self.__hdfsURI + "/" + str(key), \
            self.__ioOptions["inputFormatClass"], \
            self.__ioOptions["keyClass"], self.__ioOptions["valueClass"])
          self.__map[key] = rdd.collectAsMap()
        except:
          # no such an index
          self.__map[key] = dict()
      else:
        self.__map[key] = dict()
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
        rdd.saveAsNewAPIHadoopFile(self.__outURI + "/" + str(key), \
          self.__ioOptions["outputFormatClass"], \
          self.__ioOptions["keyClass"], self.__ioOptions["valueClass"])

  def keys(self):
    if self.__existingKeys:
      return set().union(self.__map.keys(), self.__existingKeys)
    else:
      return set(self.__map.keys())
