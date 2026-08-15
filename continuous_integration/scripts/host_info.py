import decimal

import psutil

from dask.utils import format_bytes

from distributed.metrics import monotonic
from distributed.utils import get_ip, get_ipv6
from distributed.utils_test import has_ipv6


def bench(deadline: float = 0.1) -> float:
    """Very crude CPU benchmark.
    Calculate as many digits of pi as possible within deadline seconds.
    Return how many digits were calculated per second.
    """
    t0 = monotonic()
    digits = 0
    precision = 16
    while True:
        decimal.getcontext().prec = precision
        c = decimal.Decimal(426880) * decimal.Decimal(10005).sqrt()
        m = 1
        l = 13591409
        x = 1
        k = 6
        s = decimal.Decimal(l)
        for i in range(1, precision // 14 + 2):
            m = (m * (k**3 - 16 * k)) // (i**3)
            l += 545140134
            x *= -262537412640768000
            s += decimal.Decimal(m * l) / x
            k += 12

        pi = c / s
        digits = max(digits, len(str(pi).replace(".", "")) - 1)

        t1 = monotonic()
        if t1 - t0 > deadline:
            return digits / (t1 - t0)
        precision += 10


def main() -> None:
    print(f"Number of CPUs: {psutil.cpu_count()}")

    # Run the benchmark twice and throw away the first result;
    # this gives adaptive scaling CPUs time to spin up
    bench()
    print(f"Crude CPU benchmark (higher is better): {bench():.1f}")

    try:
        freqs = psutil.cpu_freq(percpu=True)
    # https://github.com/giampaolo/psutil/issues/2382
    except (AttributeError, RuntimeError):
        print("CPU frequency: not available")
    else:
        print("CPU frequency:")
        for freq in freqs:
            print(f"  - current={freq.current}, min={freq.min}, max={freq.max}")

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
