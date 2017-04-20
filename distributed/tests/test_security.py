from __future__ import print_function, division, absolute_import

import ssl

import pytest
from tornado import gen

from distributed.comm import connect, listen
from distributed.security import Security
from distributed.utils_test import new_config, get_cert, gen_test


ca_file = get_cert('tls-ca-cert.pem')

cert1 = get_cert('tls-cert.pem')
key1 = get_cert('tls-key.pem')
keycert1 = get_cert('tls-key-cert.pem')


def test_defaults():
    with new_config({}):
        sec = Security()
    assert sec.tls_ca_file is None
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key is None
    assert sec.tls_scheduler_cert is None
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert is None


def test_attribute_error():
    sec = Security()
    assert hasattr(sec, 'tls_ca_file')
    with pytest.raises(AttributeError):
        sec.tls_foobar
    with pytest.raises(AttributeError):
        sec.tls_foobar = ""


def test_from_config():
    c = {
        'tls': {
            'ca-file': 'ca.pem',
            'scheduler': {
                'key': 'skey.pem',
                'cert': 'scert.pem',
                },
            'worker': {
                'cert': 'wcert.pem',
                },
            },
        }
    with new_config(c):
        sec = Security()
    assert sec.tls_ca_file == 'ca.pem'
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key == 'skey.pem'
    assert sec.tls_scheduler_cert == 'scert.pem'
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert == 'wcert.pem'


def test_kwargs():
    c = {
        'tls': {
            'ca-file': 'ca.pem',
            'scheduler': {
                'key': 'skey.pem',
                'cert': 'scert.pem',
                },
            },
        }
    with new_config(c):
        sec = Security(tls_scheduler_cert='newcert.pem')
    assert sec.tls_ca_file == 'ca.pem'
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key == 'skey.pem'
    assert sec.tls_scheduler_cert == 'newcert.pem'
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert is None


def test_repr():
    with new_config({}):
        sec = Security(tls_ca_file='ca.pem', tls_scheduler_cert='scert.pem')
        assert repr(sec) == "Security(tls_ca_file='ca.pem', tls_scheduler_cert='scert.pem')"


def test_tls_config_for_role():
    c = {
        'tls': {
            'ca-file': 'ca.pem',
            'scheduler': {
                'key': 'skey.pem',
                'cert': 'scert.pem',
                },
            'worker': {
                'cert': 'wcert.pem',
                },
            },
        }
    with new_config(c):
        sec = Security()
    t = sec.get_tls_config_for_role('scheduler')
    assert t == {
        'ca_file': 'ca.pem',
        'key': 'skey.pem',
        'cert': 'scert.pem',
        }
    t = sec.get_tls_config_for_role('worker')
    assert t == {
        'ca_file': 'ca.pem',
        'key': None,
        'cert': 'wcert.pem',
        }
    t = sec.get_tls_config_for_role('client')
    assert t == {
        'ca_file': 'ca.pem',
        'key': None,
        'cert': None,
        }
    with pytest.raises(ValueError):
        sec.get_tls_config_for_role('supervisor')


def test_connection_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname == False

    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
                },
            'worker': {
                'cert': keycert1,
                },
            },
        }
    with new_config(c):
        sec = Security()

    d = sec.get_connection_args('scheduler')
    assert set(d) == {'ssl_context'}
    ctx = d['ssl_context']
    basic_checks(ctx)

    d = sec.get_connection_args('worker')
    assert set(d) == {'ssl_context'}
    ctx = d['ssl_context']
    basic_checks(ctx)

    # No cert defined => no TLS
    d = sec.get_connection_args('client')
    assert d.get('ssl_context') is None


def test_listen_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname == False

    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
                },
            'worker': {
                'cert': keycert1,
                },
            },
        }
    with new_config(c):
        sec = Security()

    d = sec.get_listen_args('scheduler')
    assert set(d) == {'ssl_context'}
    ctx = d['ssl_context']
    basic_checks(ctx)

    d = sec.get_listen_args('worker')
    assert set(d) == {'ssl_context'}
    ctx = d['ssl_context']
    basic_checks(ctx)

    # No cert defined => no TLS
    d = sec.get_listen_args('client')
    assert d.get('ssl_context') is None


@gen_test()
def test_tls_listen_connect():
    """
    Simple functional test for TLS connection args.
    """
    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
                },
            'worker': {
                'cert': keycert1,
                },
            },
        }
    with new_config(c):
        sec = Security()

    @gen.coroutine
    def handle_comm(comm):
        peer_addr = comm.peer_address
        assert peer_addr.startswith('tls://')
        yield comm.write('hello')
        yield comm.close()

    with listen('tls://', handle_comm,
                connection_args=sec.get_listen_args('scheduler')) as listener:
        comm = yield connect(listener.contact_address,
                             connection_args=sec.get_connection_args('worker'))
        msg = yield comm.read()
        assert msg == 'hello'
        comm.abort()

        # No SSL context for client
        with pytest.raises(TypeError):
            yield connect(listener.contact_address,
                          connection_args=sec.get_connection_args('client'))
