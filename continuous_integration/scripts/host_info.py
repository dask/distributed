from __future__ import annotations

import numpy
import psutil

from dask.utils import format_bytes

from distributed.utils import get_ip, get_ipv6, time
from distributed.utils_test import has_ipv6


def bench() -> float:
    t0 = time()
    i = 0
    while True:
        a = numpy.random.random((1000, 1000))
        (a @ a.T).sum()

        i += 1
        t1 = time()
        if t1 - t0 > 1:
            return i / (t1 - t0)


def main() -> None:
    print(f"Number of CPUs: {psutil.cpu_count()}")

    # Run the benchmark twice and throw away the first result;
    # this gives adaptive scaling CPUs time to spin up
    bench()
    print(f"Crude CPU benchmark (higher is better): {bench():.1f}")

    freqs = psutil.cpu_freq(percpu=True)
    print("CPU frequency:")
    for freq in freqs:
        # FIXME types-psutil
        print(f"  - current={freq.current}, min={freq.min}, max={freq.max}")  # type: ignore

    mem = psutil.virtual_memory()
    print("Memory:")
    for name in dir(mem):
        if name.startswith("_"):
            continue
        v = getattr(mem, name)
        if isinstance(v, int):
            print(f"  - {name:<9} = {format_bytes(v):>10}")

    print(f"IPv4: {get_ip()}")
    print(f"IPv6: {get_ipv6() if has_ipv6() else 'unavailable'}")


if __name__ == "__main__":
    main()
