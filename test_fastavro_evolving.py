from io import BytesIO

import pytest
from fastavro.read import schemaless_reader
from fastavro.schema import parse_schema
from fastavro.write import schemaless_writer


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    schemaless_writer(stream, schema, data)
    stream.seek(0)
    return stream.read()


def deserialize(data: bytes, schema) -> dict:
    stream = BytesIO(data)
    return schemaless_reader(stream, schema)


schema_v1 = parse_schema(
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

schema_v2 = parse_schema(
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
