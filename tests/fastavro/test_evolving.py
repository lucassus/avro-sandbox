from io import BytesIO

from fastavro.read import schemaless_reader
from fastavro.write import schemaless_writer

from tests.fastavro.schemas import schema_v1, schema_v2


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    schemaless_writer(stream, schema, data)
    stream.seek(0)
    return stream.read()


def deserialize(data: bytes, schema) -> dict:
    stream = BytesIO(data)
    return schemaless_reader(stream, schema)


def test_deserialize_v1():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
        },
        schema_v1,
    )
    assert len(data) == 10

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
