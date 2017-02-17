from distributed.cli.utils import uri_from_host_port

def test_uri_from_host_port():
    addr = uri_from_host_port('127.0.0.1', 0, 8786)
    port = addr.split(':')[-1]
    assert port
    assert port == '0'
