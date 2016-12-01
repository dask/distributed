from collections import deque
import psutil
from time import time


class SystemMonitor(object):
    def __init__(self, n=1000):
        self.proc = psutil.Process()

        self.time = deque(maxlen=n)
        self.cpu = deque(maxlen=n)
        self.memory = deque(maxlen=n)
        self.num_fds = deque(maxlen=n)
        self.read_bytes = deque(maxlen=n)
        self.write_bytes = deque(maxlen=n)

        self.last_time = time()
        self.count = 0

        self._last_io_counters = self.proc.io_counters()
        self.quantities = {'cpu': self.cpu,
                           'memory': self.memory,
                           'time': self.time,
                           'num_fds': self.num_fds,
                           'read_bytes': self.read_bytes,
                           'write_bytes': self.write_bytes}

    def update(self):
        cpu = self.proc.cpu_percent()
        memory = self.proc.memory_info().rss
        num_fds = self.proc.num_fds()

        now = time()
        ioc = self.proc.io_counters()
        last = self._last_io_counters
        read_bytes = (ioc.read_bytes - last.read_bytes) / (now - self.last_time)
        write_bytes = (ioc.write_bytes - last.write_bytes) / (now - self.last_time)
        self._last_io_counters = ioc
        self.last_time = now

        self.cpu.append(cpu)
        self.memory.append(memory)
        self.num_fds.append(num_fds)
        self.time.append(now)

        self.read_bytes.append(read_bytes)
        self.write_bytes.append(write_bytes)

        self.count += 1

        return {'cpu': cpu, 'memory': memory, 'num_fds': num_fds, 'time': now,
                'count': self.count, 'read_bytes': read_bytes, 'write_bytes':
                write_bytes}
