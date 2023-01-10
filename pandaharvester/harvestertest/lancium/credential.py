import os
from lancium.api.Data import Data
from constants import voms_local_path, voms_lancium_path

def fake_callback(total_chunks, current_chunk):
    pass

# https://lancium.github.io/compute-api-docs/library/lancium/api/Data.html#Data.create
data = Data().create(voms_lancium_path, 'file', source=os.path.abspath(voms_local_path), force=True)
data.upload(os.path.abspath(voms_local_path), fake_callback)

#  TODO: see what happens if you create and upload it multiple times,
#   or if you need to list it before to validate the existence

ex = data.show(voms_id)[0]
print(ex.__dict__)
