import os
from lancium.api.Data import Data
from constants import voms_local_path, voms_lancium_path, script_local_path, script_lancium_path


def fake_callback(total_chunks, current_chunk):
    pass


# https://lancium.github.io/compute-api-docs/library/lancium/api/Data.html#Data.create

# 1. Upload a fake voms proxy
data = Data().create(voms_lancium_path, "file", source=os.path.abspath(voms_local_path), force=True)
data.upload(os.path.abspath(voms_local_path), fake_callback)
ex = data.show(voms_lancium_path)[0]
print(ex.__dict__)

# 2. Upload the pilot starter
data = Data().create(script_lancium_path, "file", source=os.path.abspath(script_local_path), force=True)
data.upload(os.path.abspath(script_local_path), fake_callback)
ex = data.show(script_lancium_path)[0]
print(ex.__dict__)
