import json
from io import BytesIO

import avro.schema
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter


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


schema_v1 = avro.schema.parse(
    json.dumps(
        {
            "namespace": "sandbox",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": ["int", "null"]},
            ],
        }
    )
)

schema_v2 = avro.schema.parse(
    json.dumps(
        {
            "namespace": "sandbox",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": ["int", "null"]},
                {"name": "favorite_color", "type": "string", "default": "green"},
            ],
        }
    )
)


def test_deserialize_v1():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
        },
        schema_v1,
    )

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
