from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

from distributed.cli.utils import uri_from_host_port
from distributed.utils import get_ip


external_ip = get_ip()


def test_uri_from_host_port():
    f = uri_from_host_port

    assert f('', 456, None) == ('tcp://:456', external_ip)
    assert f('', 456, 123) == ('tcp://:456', external_ip)
    assert f('', None, 123) == ('tcp://:123', external_ip)
    assert f('', None, 0) == ('tcp://', external_ip)

    assert f('localhost', 456, None) == ('tcp://localhost:456', '127.0.0.1')
    assert f('localhost', 456, 123) == ('tcp://localhost:456', '127.0.0.1')
    assert f('localhost', None, 123) == ('tcp://localhost:123', '127.0.0.1')
    assert f('localhost', None, 0) == ('tcp://localhost', '127.0.0.1')

    assert f('192.168.1.2', 456, None) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('192.168.1.2', 456, 123) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('192.168.1.2', None, 123) == ('tcp://192.168.1.2:123', '192.168.1.2')
    assert f('192.168.1.2', None, 0) == ('tcp://192.168.1.2', '192.168.1.2')

    assert f('tcp://192.168.1.2', 456, None) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('tcp://192.168.1.2', 456, 123) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('tcp://192.168.1.2', None, 123) == ('tcp://192.168.1.2:123', '192.168.1.2')
    assert f('tcp://192.168.1.2', None, 0) == ('tcp://192.168.1.2', '192.168.1.2')

    assert f('tcp://192.168.1.2:456', None, None) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('tcp://192.168.1.2:456', 0, 0) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('tcp://192.168.1.2:456', 0, 123) == ('tcp://192.168.1.2:456', '192.168.1.2')
    assert f('tcp://192.168.1.2:456', 456, 123) == ('tcp://192.168.1.2:456', '192.168.1.2')

    with pytest.raises(ValueError):
        f('tcp://192.168.1.2:456', 123, None)

    assert f('zmq://192.168.1.2:456', None, None) == ('zmq://192.168.1.2:456', '192.168.1.2')
    assert f('zmq://192.168.1.2:456', 0, 0) == ('zmq://192.168.1.2:456', '192.168.1.2')
    assert f('zmq://192.168.1.2:456', 0, 123) == ('zmq://192.168.1.2:456', '192.168.1.2')
    assert f('zmq://192.168.1.2:456', 456, 123) == ('zmq://192.168.1.2:456', '192.168.1.2')

    assert f('tcp://[::1]:456', None, None) == ('tcp://[::1]:456', '::1')
