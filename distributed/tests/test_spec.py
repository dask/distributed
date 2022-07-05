from __future__ import annotations

from distributed.deploy.spec import ProcessInterface
from distributed.utils_test import gen_test


@gen_test()
async def test_address_default_none():
    async with ProcessInterface() as p:
        assert p.address is None


@gen_test()
async def test_child_address_persists():
    class Child(ProcessInterface):
        def __init__(self, address=None):
            self.address = address
            super().__init__()

    async with Child() as c:
        assert c.address is None

    async with Child("localhost") as c:
        assert c.address == "localhost"
