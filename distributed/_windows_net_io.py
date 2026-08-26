# Windows-only fast path for retrieving net_io_counters.
# Part of fix for Issue #9161 (high CPU when idle, caused by expensive psutil.net_io_counters on Windows).

import ctypes
import logging
import sys
from collections import namedtuple

if sys.platform == "win32":
    from ctypes import wintypes

logger = logging.getLogger("distributed.system_monitor")

# Define GAA flags to skip unnecessary data and speed up the call
GAA_FLAG_SKIP_UNICAST = 0x0001
GAA_FLAG_SKIP_ANYCAST = 0x0002
GAA_FLAG_SKIP_MULTICAST = 0x0004
GAA_FLAG_SKIP_DNS_SERVER = 0x0008

# Define compatible namedtuple for psutil's snetio
win_net_io_counters = namedtuple(
    "win_net_io_counters",
    [
        "bytes_sent",
        "bytes_recv",
        "packets_sent",
        "packets_recv",
        "errin",
        "errout",
        "dropin",
        "dropout",
    ],
)


class IP_ADAPTER_ADDRESSES(ctypes.Structure):
    pass


# We only define fields up to FriendlyName. Since we only read the memory
# returned by GetAdaptersAddresses, this is safe and avoids mapping all fields.
IP_ADAPTER_ADDRESSES._fields_ = [
    ("Length", ctypes.c_ulong),
    ("IfIndex", ctypes.c_ulong),
    ("Next", ctypes.POINTER(IP_ADAPTER_ADDRESSES)),
    ("AdapterName", ctypes.c_char_p),
    ("FirstUnicastAddress", ctypes.c_void_p),
    ("FirstAnycastAddress", ctypes.c_void_p),
    ("FirstMulticastAddress", ctypes.c_void_p),
    ("FirstDnsServerAddress", ctypes.c_void_p),
    ("DnsSuffix", ctypes.c_wchar_p),
    ("Description", ctypes.c_wchar_p),
    ("FriendlyName", ctypes.c_wchar_p),
]


class MIB_IF_ROW2(ctypes.Structure):
    _fields_ = [
        ("InterfaceLuid", ctypes.c_uint64),
        ("InterfaceIndex", ctypes.c_ulong),
        ("InterfaceGuid", ctypes.c_byte * 16),
        ("Alias", ctypes.c_wchar * 257),
        ("Description", ctypes.c_wchar * 257),
        ("PhysicalAddressLength", ctypes.c_ulong),
        ("PhysicalAddress", ctypes.c_byte * 32),
        ("PermanentPhysicalAddress", ctypes.c_byte * 32),
        ("Mtu", ctypes.c_ulong),
        ("Type", ctypes.c_ulong),
        ("TunnelType", ctypes.c_ulong),
        ("MediaType", ctypes.c_ulong),
        ("PhysicalMediumType", ctypes.c_ulong),
        ("AccessType", ctypes.c_ulong),
        ("DirectionType", ctypes.c_ulong),
        ("InterfaceAndOperStatusFlags", ctypes.c_byte),
        # 3 bytes padding (added automatically by ctypes alignment)
        ("OperStatus", ctypes.c_ulong),
        ("AdminStatus", ctypes.c_ulong),
        ("MediaConnectState", ctypes.c_ulong),
        ("NetworkGuid", ctypes.c_byte * 16),
        ("ConnectionType", ctypes.c_ulong),
        # 4 bytes padding (added automatically by ctypes alignment)
        ("TransmitLinkSpeed", ctypes.c_uint64),
        ("ReceiveLinkSpeed", ctypes.c_uint64),
        ("InOctets", ctypes.c_uint64),
        ("InUcastPkts", ctypes.c_uint64),
        ("InNUcastPkts", ctypes.c_uint64),
        ("InDiscards", ctypes.c_uint64),
        ("InErrors", ctypes.c_uint64),
        ("InUnknownProtos", ctypes.c_uint64),
        ("InUcastOctets", ctypes.c_uint64),
        ("InMulticastOctets", ctypes.c_uint64),
        ("InBroadcastOctets", ctypes.c_uint64),
        ("OutOctets", ctypes.c_uint64),
        ("OutUcastPkts", ctypes.c_uint64),
        ("OutNUcastPkts", ctypes.c_uint64),
        ("OutDiscards", ctypes.c_uint64),
        ("OutErrors", ctypes.c_uint64),
        ("OutUcastOctets", ctypes.c_uint64),
        ("OutMulticastOctets", ctypes.c_uint64),
        ("OutBroadcastOctets", ctypes.c_uint64),
        ("OutQLen", ctypes.c_uint64),
    ]


# Setup Windows API calls
if sys.platform == "win32":
    iphlpapi = ctypes.windll.iphlpapi

    GetAdaptersAddresses = iphlpapi.GetAdaptersAddresses
    GetAdaptersAddresses.argtypes = [
        wintypes.ULONG,
        wintypes.ULONG,
        ctypes.c_void_p,
        ctypes.POINTER(IP_ADAPTER_ADDRESSES),
        ctypes.POINTER(wintypes.ULONG),
    ]
    GetAdaptersAddresses.restype = wintypes.ULONG

    GetIfEntry2 = iphlpapi.GetIfEntry2
    GetIfEntry2.argtypes = [ctypes.POINTER(MIB_IF_ROW2)]
    GetIfEntry2.restype = wintypes.ULONG
else:
    iphlpapi = None
    GetAdaptersAddresses = None
    GetIfEntry2 = None


# Cached buffer size for GetAdaptersAddresses
_ADAPTER_ADDRESSES_BUF_SIZE = 16384


def _fast_net_io_counters() -> win_net_io_counters:
    """Low-overhead Windows-only network I/O stats querying using Win32 API."""
    global _ADAPTER_ADDRESSES_BUF_SIZE
    size = wintypes.ULONG(_ADAPTER_ADDRESSES_BUF_SIZE)  # type: ignore[name-defined]
    buf = ctypes.create_string_buffer(size.value)

    flags = (
        GAA_FLAG_SKIP_UNICAST
        | GAA_FLAG_SKIP_ANYCAST
        | GAA_FLAG_SKIP_MULTICAST
        | GAA_FLAG_SKIP_DNS_SERVER
    )

    ret = GetAdaptersAddresses(  # type: ignore[misc]
        0,  # AF_UNSPEC
        flags,
        None,
        ctypes.cast(buf, ctypes.POINTER(IP_ADAPTER_ADDRESSES)),
        ctypes.byref(size),
    )

    # If the buffer was too small, update the cached size, allocate, and call again
    if ret == 111:  # ERROR_BUFFER_OVERFLOW
        _ADAPTER_ADDRESSES_BUF_SIZE = size.value
        buf = ctypes.create_string_buffer(size.value)
        ret = GetAdaptersAddresses(  # type: ignore[misc]
            0,
            flags,
            None,
            ctypes.cast(buf, ctypes.POINTER(IP_ADAPTER_ADDRESSES)),
            ctypes.byref(size),
        )

    if ret != 0:
        raise OSError(f"GetAdaptersAddresses failed with error {ret}")

    curr_ptr = ctypes.cast(buf, ctypes.POINTER(IP_ADAPTER_ADDRESSES))

    bytes_recv = 0
    bytes_sent = 0
    packets_recv = 0
    packets_sent = 0
    errin = 0
    errout = 0
    dropin = 0
    dropout = 0

    while curr_ptr:
        curr = curr_ptr.contents
        ifIndex = curr.IfIndex

        row = MIB_IF_ROW2()
        row.InterfaceIndex = ifIndex

        status = GetIfEntry2(ctypes.byref(row))  # type: ignore[misc]
        if status == 0:
            bytes_recv += row.InOctets
            bytes_sent += row.OutOctets
            packets_recv += row.InUcastPkts + row.InNUcastPkts
            packets_sent += row.OutUcastPkts + row.OutNUcastPkts
            errin += row.InErrors
            errout += row.OutErrors
            dropin += row.InDiscards
            dropout += row.OutDiscards

        curr_ptr = curr.Next

    return win_net_io_counters(
        bytes_sent=bytes_sent,
        bytes_recv=bytes_recv,
        packets_sent=packets_sent,
        packets_recv=packets_recv,
        errin=errin,
        errout=errout,
        dropin=dropin,
        dropout=dropout,
    )


def fast_net_io_counters():
    """Wrapper that falls back to psutil.net_io_counters on error."""
    try:
        return _fast_net_io_counters()
    except Exception as e:
        logger.debug(
            "Windows fast path net_io_counters failed, falling back to psutil: %r",
            e,
            exc_info=True,
        )
        import psutil

        return psutil.net_io_counters()
