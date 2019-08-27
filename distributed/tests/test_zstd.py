import os, pytest


@pytest.mark.parametrize(
    "cfg,values",
    [
        (
            "distributed.comm.zstd.import_policy",
            ["default", "cffi_fallback", "cext", "cffi"],
        ),
        ("distributed.comm.zstd.level", range(1, 23)),
        ("distributed.comm.zstd.write_checksum", [False, True]),
        ("distributed.comm.zstd.write_content_size", [True]),
        ("distributed.comm.zstd.write_dict_id", [False, True]),
        ("distributed.comm.zstd.threads", range(-1, 100, 1)),
    ],
)
def test_zstd(cfg, values):
    zstandard = pytest.importorskip("zstandard")
    import zstandard as zstd, importlib, cffi, dask, distributed.protocol.compression as compr

    original_data = os.urandom(100)

    for v in values:
        with dask.config.set({cfg: v}):
            importlib.reload(cffi)
            importlib.reload(zstandard)
            importlib.reload(zstd)
            importlib.reload(compr)
            compress, decompress = (
                compr.compressions["zstd"]["compress"],
                compr.compressions["zstd"]["decompress"],
            )
            data = decompress(compress(original_data))
            assert data == original_data, "config:%s, value:%s" % (cfg, v)
