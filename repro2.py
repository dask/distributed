from distributed.protocol.serialize import dask_dumps, dask_loads
import numpy as np

def main():
    header, frames = dask_dumps([0,1,2])
    print(header, frames)

    x = dask_loads(header, frames)
    print(x)

    header2, frames2 = dask_dumps(np.array([0,1,2]))
    print(header2, frames2)

    x = dask_loads(header2, frames2)
    print(x)

    print('done')

if __name__ == "__main__":
    main()