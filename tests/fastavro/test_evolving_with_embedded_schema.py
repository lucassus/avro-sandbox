from io import BytesIO

from fastavro.read import reader
from fastavro.write import writer

from tests.fastavro.schemas import schema_v1, schema_v2


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    writer(stream, schema, records=[data])
    stream.seek(0)
    return stream.read()


def deserialize(data: bytes, schema) -> dict:
    stream = BytesIO(data)
    return next(reader(stream, schema))


def test_deserialize_v1():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
        },
        schema_v1,
    )
    assert len(data) == 224

    assert deserialize(data, schema_v1) == {
        "name": "Alyssa",
        "favorite_number": 256,
    }


def test_deserialize_v2():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
        },
        schema_v1,
    )

    assert deserialize(data, schema_v2) == {
        "name": "Alyssa",
        "favorite_number": 256,
        "favorite_color": "green",
    }
