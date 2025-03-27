import json
from io import BytesIO

import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

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


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    writer = DataFileWriter(stream, DatumWriter(), schema)
    writer.append(data)
    writer.flush()
    serialized = stream.getvalue()
    writer.close()
    return serialized


def deserialize(data: bytes, schema) -> dict:
    stream = BytesIO(data)
    reader = DataFileReader(stream, DatumReader(readers_schema=schema))
    return next(reader)


def test_deserialize_v1():
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
