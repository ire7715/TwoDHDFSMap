class TwoDHDFSMap(object):
  __SLOT_SIZE = 100

  def __init__(self, sc, hdfsURI=None, outURI=None):
    self.__hdfsURI = str(hdfsURI) if hdfsURI else None
    self.__sc = sc
    self.__map = dict()
    self.__outURI = str(outURI) if outURI else None

    if self.__hdfsURI:
      self.__slotsRead = [False] * self.__SLOT_SIZE

  @property
  def hdfsURI(self):
      return self.__hdfsURI

  def __readFromHash(self, keyHash):
    rdd = self.__sc.pickleFile(self.__hdfsURI + "/" + str(keyHash))
    # dataBlock = rdd.combineByKey(lambda v: [v], lambda c, v: c + [v], \
    #   lambda c1, c2: c1 + c2).collectAsMap()
    dataBlock = rdd.collectAsMap()
    for key, tuples in dataBlock.iteritems():
      self.__map[key] = dict(tuples)
    self.__slotsRead[keyHash] = True

  def __getitem__(self, key):
    if isinstance(key, str) and "/" in key:
      raise KeyError("A slash(/) in the key(\"" + key + "\") is not allowed.")
    if key not in self.__map:
      keyHash = self.__keyHash(key)
      if self.__hdfsURI and not self.__slotsRead[keyHash]:
        try:
          self.__readFromHash(keyHash)
          if key not in self.__map:
            self.__map[key] = dict()
        except:
          # no such an index
          self.__slotsRead[keyHash] = True
          self.__map[key] = dict()
      else:
        self.__map[key] = dict()
    return self.__map[key]

  def __setitem__(self, key, value):
    self[key]
    self.__map[key] = value
    return value

  def __contains__(self, key):
    self[key]
    return key in self.__map

  def save(self):
    if self.__outURI:
      distribution = [[]] * self.__SLOT_SIZE
      for key, value in self.__map.iteritems():
        keyHash = self.__keyHash(key)
        distribution[keyHash].append((key, value))

      for index, block in enumerate(distribution):
        self.__sc.parallelize(block) \
        .saveAsPickleFile(self.__outURI + "/" + str(index))

  def __del__(self):
    self.save()

  def __keyHash(self, key):
    # TODO define a proper slot size and a proper hash funciton
    return hash(key) % self.__SLOT_SIZE
