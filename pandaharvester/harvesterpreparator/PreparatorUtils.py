import hashlib


# construct file path
def constructFilePath(basePath, datasetName, scope, lfn):
    hash = hashlib.md5()
    hash.update('%s:%s' % (scope, lfn))
    hash_hex = hash.hexdigest()
    correctedscope = "/".join(scope.split('.'))
    dstURL = "{basePath}/{scope}/{hash1}/{hash2}/{lfn}".format(basePath=basePath,
                                                               scope=correctedscope,
                                                               hash1=hash_hex[0:2],
                                                               hash2=hash_hex[2:4],
                                                               lfn=lfn)
    return dstURL
