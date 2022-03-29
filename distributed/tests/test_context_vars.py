from distributed.context_vars import STIMULUS_ID


def a_very_underlined_function():
    return STIMULUS_ID.get()


def test_stimulus_id():
    assert a_very_underlined_function().startswith("a-very-underlined-function")
