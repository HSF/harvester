import hashlib


# construct file path
def construct_file_path(base_path, scope, lfn):
    hash = hashlib.md5()
    hash.update(f"{scope}:{lfn}".encode("latin_1"))
    hash_hex = hash.hexdigest()
    correctedscope = "/".join(scope.split("."))
    dstURL = f"{base_path}/{correctedscope}/{hash_hex[0:2]}/{hash_hex[2:4]}/{lfn}"
    return dstURL
