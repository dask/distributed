from distributed.protocol import serialize, deserialize
import pytest

np = pytest.importorskip('numpy')
torch = pytest.importorskip('torch')


def test_tensor():
    x = np.arange(10)
    t = torch.Tensor(x)
    header, frames = serialize(t)
    assert header['serializer'] == 'dask'
    t2 = deserialize(header, frames)
    assert (x == t2.numpy()).all()


def test_resnet():
    torchvision = pytest.importorskip('torchvision')
    model = torchvision.models.resnet.resnet18()

    header, frames = serialize(model)
    import pdb; pdb.set_trace()
    model2 = deserialize(header, frames)
    assert str(model) == str(model2)
