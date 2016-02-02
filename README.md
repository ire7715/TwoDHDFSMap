# TwoDHDFSMap
A library for hdfs 2d dictionary with PySpark  
This project is dealing 2-dimensional dictionary, while you don't want to read the whole dictionary file. It automatically separate files by the key hash, and apply the lazy load strategy.

## Document

#### __init__  
&nbsp;parameters:  
&nbsp;&nbsp;*sc*: **required** SparkContext  
&nbsp;&nbsp;hdfsURI=None: URI to HDFS file  
&nbsp;&nbsp;outURI=None: the output file name. If it's `None`, it would not generate an output file. Otherwise, it would generate a output file at given URI when destruction.
&nbsp;&nbsp;bucketSize=0: Hash buckets size, you can set a peoper bucket size for your application.

supports get(m[k]), set(m[k] = v), in(k in m) operators, 
#### Get & set operations
```
  m[0][1] = 2
  print("[0][1] of the map is " + str(m[0][1]))
```

#### In operation
```
  print("There is " + str("" if "hey" in m else "not ") + "a key named \"hey\" in the map.")
```

#### save()
```
  # This function only works when the outURI is set.
  m.save()
```

#### retrieveAll()
```
  # This function retrieve all key from HDFS
```

#### keys()
```
  # Read all keys of this dictionary, it automatically call retrieveAll()
```

#### toDataFrame()
```
  # Return the pandas.DataFrame of this dictionary.
```
