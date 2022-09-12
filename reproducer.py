import numpy as np
import dask
import distributed
from distributed import Client
from distributed.protocol.keras import deserialize_keras_model, serialize_keras_model
import keras


def main():
    def build_model():
        inp = keras.layers.Input(shape=(1,))
        out = keras.layers.Dense(1, activation='linear')(inp)
        model = keras.models.Model(inputs=inp, outputs=out)
        return model    

    @dask.delayed
    def get_keras_model_delayed():
        return build_model()

    def get_keras_model():
        return build_model()

    client = Client()

    data = np.random.randint(0, 100, size=(15, 1))

    model = get_keras_model()
    headers, frames = serialize_keras_model(model)
    deserialized_model = deserialize_keras_model(headers, frames)

    assert (deserialized_model.predict(data) == model.predict(data)).all()

    model = get_keras_model_delayed().compute() # this line raise an exception

if __name__ == "__main__":
    main()