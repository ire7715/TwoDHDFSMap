from TwoDHDFSMap import TwoDHDFSMap

# read-only
class RDDMap(object):
  def __init__(self, sc, rddURI, bucketSize=101):
    self.__rddURI = rddURI
    self.__sc = sc
    self.__bucketSize = bucketSize
    self.__slotsRead = [None] * bucketSize
    self.__map = dict()

  def __readRDD(self, keyHash):
    rdd = self.__sc.pickleFile(self.__rddURI + "/" + str(keyHash) + ".2drdd")
    self.__slotsRead[keyHash] = rdd
    return rdd

  def __keyHash(self, key):
    return hash(key) % self.__bicketSize

  def __getitem__(self, key):
    keyHash = self.__keyHash(key)
    if not self.__slotsRead[keyHash]:
      rdd = self.__readRDD(keyHash)
      blockDict = rdd.map(lambda row: (row[0][0], (row[0][1], row[1]))) \
      .reductByKey(lambda m, n: [m] + [n]).collectAsMap()
      for key, value in blockDict.iteritems():
        self.__map[key] = self.__sc.parallelize(value)
    return self.__map[key]
