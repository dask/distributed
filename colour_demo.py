from distributed import Client
import dask.array as da
import dask
import dask.dataframe as dd
import dask.config

if __name__=="__main__":
    dask.config.set({"distributed.worker.memory.target":0.2})
    dask.config.set({"distributed.worker.memory.spill":0.3})

    client = Client()


    x = da.random.random((30000, 30000), chunks=(300, 300))
    y = x + x.T
    z = y[::2, 50:].mean(axis=1)

    bc = z.compute()