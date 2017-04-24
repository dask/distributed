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

# Note this cipher uses RSA auth as this matches our test certs
FORCED_CIPHER = 'ECDHE-RSA-AES128-GCM-SHA256'


def test_defaults():
    with new_config({}):
        sec = Security()
    assert sec.tls_ca_file is None
    assert sec.tls_ciphers is None
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
            'ciphers': FORCED_CIPHER,
            },
        }
    with new_config(c):
        sec = Security()
    assert sec.tls_ca_file == 'ca.pem'
    assert sec.tls_ciphers == FORCED_CIPHER
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
    assert sec.tls_ciphers is None
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
            'ciphers': FORCED_CIPHER,
            },
        }
    with new_config(c):
        sec = Security()
    t = sec.get_tls_config_for_role('scheduler')
    assert t == {
        'ca_file': 'ca.pem',
        'key': 'skey.pem',
        'cert': 'scert.pem',
        'ciphers': FORCED_CIPHER,
        }
    t = sec.get_tls_config_for_role('worker')
    assert t == {
        'ca_file': 'ca.pem',
        'key': None,
        'cert': 'wcert.pem',
        'ciphers': FORCED_CIPHER,
        }
    t = sec.get_tls_config_for_role('client')
    assert t == {
        'ca_file': 'ca.pem',
        'key': None,
        'cert': None,
        'ciphers': FORCED_CIPHER,
        }
    with pytest.raises(ValueError):
        sec.get_tls_config_for_role('supervisor')


def test_connection_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname == False

    def many_ciphers(ctx):
        assert len(ctx.get_ciphers()) > 2  # Most likely

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
    many_ciphers(ctx)

    d = sec.get_connection_args('worker')
    assert set(d) == {'ssl_context'}
    ctx = d['ssl_context']
    basic_checks(ctx)
    many_ciphers(ctx)

    # No cert defined => no TLS
    d = sec.get_connection_args('client')
    assert d.get('ssl_context') is None

    # With a cipher string
    c['tls']['ciphers'] = FORCED_CIPHER

    with new_config(c):
        sec = Security()

    ctx = sec.get_connection_args('scheduler')['ssl_context']
    basic_checks(ctx)
    assert len(ctx.get_ciphers()) == 1


def test_listen_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname == False

    def many_ciphers(ctx):
        assert len(ctx.get_ciphers()) > 2  # Most likely

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
    many_ciphers(ctx)

    d = sec.get_listen_args('worker')
    assert set(d) == {'ssl_context'}
    ctx = d['ssl_context']
    basic_checks(ctx)
    many_ciphers(ctx)

    # No cert defined => no TLS
    d = sec.get_listen_args('client')
    assert d.get('ssl_context') is None

    # With a cipher string
    c['tls']['ciphers'] = FORCED_CIPHER

    with new_config(c):
        sec = Security()

    ctx = sec.get_connection_args('scheduler')['ssl_context']
    basic_checks(ctx)
    assert len(ctx.get_ciphers()) == 1


@gen_test()
def test_tls_listen_connect():
    """
    Simple functional test for TLS connection args.
    """
    @gen.coroutine
    def handle_comm(comm):
        peer_addr = comm.peer_address
        assert peer_addr.startswith('tls://')
        yield comm.write('hello')
        yield comm.close()

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

    c['tls']['ciphers'] = FORCED_CIPHER
    with new_config(c):
        forced_cipher_sec = Security()

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

        # Check forced cipher
        comm = yield connect(listener.contact_address,
                             connection_args=forced_cipher_sec.get_connection_args('worker'))
        cipher, _, _, = comm.extra_info['cipher']
        assert cipher == FORCED_CIPHER
        comm.abort()
