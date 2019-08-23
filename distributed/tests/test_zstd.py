def test_zstd():
    import zstandard, zstandard as zstd, six, cffi, dask, distributed.protocol.compression as compr

    original_data = b"".join(
        [
            b"\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe5\x9b\xbd\xe4\xba\xba\xe6\xb0",
            b"\x91\xe7\x9a\x84\xe5\x84\xbf\xe5\xad\x90\xef\xbc\x8c\xe6\x88\x91\xe6",
            b"\xb7\xb1\xe6\x83\x85\xe5\x9c\xb0\xe7\x83\xad\xe7\x88\xb1\xe7\x9d\x80",
            b"\xe6\x88\x91\xe7\x9a\x84\xe7\xa5\x96\xe5\x9b\xbd\xe5\x92\x8c\xe4\xba",
            b"\xba\xe6\xb0\x91\xe3\x80\x82",
        ]
    )

    zstd_config_kwargs = [
        # 'cffi' is NOT support by "pip install zstandard"
        # but AVAILABLE by manual install zstandard with "python setup.py install"
        ("distributed.comm.zstd.import_policy", ["default", "cffi_fallback", "cext"]),
        ("distributed.comm.zstd.level", range(1, 23)),
        ("distributed.comm.zstd.write_checksum", [False, True]),
        ("distributed.comm.zstd.write_content_size", [True]),
        ("distributed.comm.zstd.write_dict_id", [False, True]),
        ("distributed.comm.zstd.threads", range(-1, 100, 1)),
    ]

    for cfg, values in zstd_config_kwargs:
        for v in values:
            with dask.config.set({cfg: v}):
                assert dask.config.get(cfg) == v, "check config:%s, value:%s" % (cfg, v)
                try:
                    six.moves.reload_module(cffi)
                    six.moves.reload_module(zstandard)
                    six.moves.reload_module(zstd)
                    six.moves.reload_module(compr)
                    compress, decompress = (
                        compr.compressions["zstd"]["compress"],
                        compr.compressions["zstd"]["decompress"],
                    )
                    data = decompress(compress(original_data))
                    if data == original_data:
                        print(
                            "PASSED, Zstandard compression, config:%s, value:%s"
                            % (cfg, v)
                        )
                        continue
                except Exception as e:
                    print(
                        "FAILED, zstandard compression, config:%s, value:%s" % (cfg, v)
                    )
                    raise


test_zstd()
