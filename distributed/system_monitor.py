from collections import deque

import psutil

from distributed.compatibility import WINDOWS
from distributed.metrics import time

try:
    from distributed.diagnostics import nvml
except Exception:
    nvml = None  # type: ignore


class SystemMonitor:
    def __init__(self, n=10000):
        self.proc = psutil.Process()

        self.time = deque(maxlen=n)
        self.cpu = deque(maxlen=n)
        self.memory = deque(maxlen=n)
        self.count = 0

        self.quantities = {"cpu": self.cpu, "memory": self.memory, "time": self.time}

        try:
            ioc = psutil.net_io_counters()
        except Exception:
            self._collect_net_io_counters = False
        else:
            self.last_time = time()
            self.read_bytes = deque(maxlen=n)
            self.write_bytes = deque(maxlen=n)
            self.quantities["read_bytes"] = self.read_bytes
            self.quantities["write_bytes"] = self.write_bytes
            self._last_io_counters = ioc
            self._collect_net_io_counters = True

        try:
            disk_ioc = psutil.disk_io_counters()
        except Exception:
            self._collect_disk_io_counters = False
        else:
            if disk_ioc is None:  # diskless machine
                self._collect_disk_io_counters = False
            else:
                self.last_time_disk = time()
                self.read_bytes_disk = deque(maxlen=n)
                self.write_bytes_disk = deque(maxlen=n)
                self.quantities["read_bytes_disk"] = self.read_bytes_disk
                self.quantities["write_bytes_disk"] = self.write_bytes_disk
                self._last_disk_io_counters = disk_ioc
                self._collect_disk_io_counters = True

        if not WINDOWS:
            self.num_fds = deque(maxlen=n)
            self.quantities["num_fds"] = self.num_fds

        if nvml.device_get_count() > 0:
            gpu_extra = nvml.one_time()
            self.gpu_name = gpu_extra["name"]
            self.gpu_memory_total = gpu_extra["memory-total"]
            self.gpu_utilization = deque(maxlen=n)
            self.gpu_memory_used = deque(maxlen=n)
            self.quantities["gpu_utilization"] = self.gpu_utilization
            self.quantities["gpu_memory_used"] = self.gpu_memory_used

        self.update()

    def recent(self):
        try:
            return {k: v[-1] for k, v in self.quantities.items()}
        except IndexError:
            return {k: None for k, v in self.quantities.items()}

    def get_process_memory(self) -> int:
        """Sample process memory, as reported by the OS.
        This one-liner function exists so that it can be easily mocked in unit tests,
        as the OS allocating and releasing memory is highly volatile and a constant
        source of flakiness.
        """
        return self.proc.memory_info().rss

    def update(self):
        with self.proc.oneshot():
            cpu = self.proc.cpu_percent()
            memory = self.get_process_memory()
        now = time()

        self.cpu.append(cpu)
        self.memory.append(memory)
        self.time.append(now)
        self.count += 1

        result = {"cpu": cpu, "memory": memory, "time": now, "count": self.count}

        if self._collect_net_io_counters:
            try:
                ioc = psutil.net_io_counters()
            except Exception:
                pass
            else:
                last = self._last_io_counters
                duration = now - self.last_time
                read_bytes = (ioc.bytes_recv - last.bytes_recv) / (duration or 0.5)
                write_bytes = (ioc.bytes_sent - last.bytes_sent) / (duration or 0.5)
                self.last_time = now
                self._last_io_counters = ioc
                self.read_bytes.append(read_bytes)
                self.write_bytes.append(write_bytes)
                result["read_bytes"] = read_bytes
                result["write_bytes"] = write_bytes

        if self._collect_disk_io_counters:
            try:
                disk_ioc = psutil.disk_io_counters()
            except Exception:
                pass
            else:
                if disk_ioc is None:  # diskless machine
                    self._collect_disk_io_counters = False
                else:
                    last_disk = self._last_disk_io_counters
                    duration_disk = now - self.last_time_disk
                    read_bytes_disk = (disk_ioc.read_bytes - last_disk.read_bytes) / (
                        duration_disk or 0.5
                    )
                    write_bytes_disk = (
                        disk_ioc.write_bytes - last_disk.write_bytes
                    ) / (duration_disk or 0.5)
                    self.last_time_disk = now
                    self._last_disk_io_counters = disk_ioc
                    self.read_bytes_disk.append(read_bytes_disk)
                    self.write_bytes_disk.append(write_bytes_disk)
                    result["read_bytes_disk"] = read_bytes_disk
                    result["write_bytes_disk"] = write_bytes_disk

        if not WINDOWS:
            num_fds = self.proc.num_fds()
            self.num_fds.append(num_fds)
            result["num_fds"] = num_fds

        if nvml.device_get_count() > 0:
            gpu_metrics = nvml.real_time()
            self.gpu_utilization.append(gpu_metrics["utilization"])
            self.gpu_memory_used.append(gpu_metrics["memory-used"])
            result["gpu_utilization"] = gpu_metrics["utilization"]
            result["gpu_memory_used"] = gpu_metrics["memory-used"]

        return result

    def __repr__(self):
        return "<SystemMonitor: cpu: %d memory: %d MB fds: %d>" % (
            self.cpu[-1],
            self.memory[-1] / 1e6,
            -1 if WINDOWS else self.num_fds[-1],
        )

    def range_query(self, start):
        if start == self.count:
            return {k: [] for k in self.quantities}

        istart = start - (self.count - len(self.cpu))
        istart = max(0, istart)

        seq = [i for i in range(istart, len(self.cpu))]

        d = {k: [v[i] for i in seq] for k, v in self.quantities.items()}
        return d
