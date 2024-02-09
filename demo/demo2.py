import dask.array as da
from dask_mpi import initialize
from distributed import Client
import time


protocol = "mpi://"
# protocol = 'tcp://'


initialize(protocol=protocol)
client = Client()


x = da.random.random((20000, 20000)).persist()
print(x._chunks)

start = time.time()
y = (x + x.T).transpose().sum().compute()
stop = time.time()

print("Seconds:", stop - start)
