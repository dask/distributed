from __future__ import print_function, division, absolute_import

import pytest

pd = pytest.importorskip('pandas')

from dask.dataframe.utils import assert_eq
from distributed.protocol import serialize, deserialize


examples = []

df = pd.DataFrame({'x': [1, 2, 3],
                   'y': ['a', 'b', 'c'],
                   'z': ['a', 'a', 'b'],
                   'w': [1.0, 2.0, 3.0]})
examples.append(df)

for col in df.columns:
    examples.append(df[col])


examples.append(pytest.mark.xfail(df.z.astype('category')))

df = pd.DataFrame({}, index=[10, 20, 30])
examples.append(pytest.mark.xfail(df))


@pytest.mark.parametrize('df', examples)
def test_simple(df):
    header, frames = serialize(df)
    assert header['type']
    df2 = deserialize(header, frames)
    assert_eq(df, df2)
