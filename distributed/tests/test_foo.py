from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
def test_1(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_2(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_3(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_4(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_5(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_6(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_7(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_8(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_9(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_10(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_11(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)


@gen_cluster(client=True)
def test_12(c, s, a, b):
    yield c.submit(lambda x: x + 1, 10)
