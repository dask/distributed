from __future__ import print_function, division, absolute_import

import pytest

from distributed.security import Security
from distributed.utils_test import new_config


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


def test_get_tls_config_for_role():
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
