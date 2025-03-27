from io import BytesIO

from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter

from tests.avro.schemas import schema_v1, schema_v2


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    encoder = BinaryEncoder(stream)
    writer = DatumWriter(schema)
    writer.write(data, encoder)
    return stream.getvalue()


def deserialize(data: bytes, schema) -> dict:
    decoder = BinaryDecoder(BytesIO(data))
    reader = DatumReader(schema)
    return reader.read(decoder)


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
