import json
from io import BytesIO

import avro.schema
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter

from tests.schemas import raw_schema_v1, raw_schema_v2

schema_v1 = avro.schema.parse(json.dumps(raw_schema_v1))

schema_v2 = avro.schema.parse(json.dumps(raw_schema_v2))


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    encoder = BinaryEncoder(stream)
    writer = DatumWriter(schema)
    writer.write(data, encoder)
    return stream.getvalue()


def deserialize(data: bytes, *, writer_schema, reader_schema) -> dict:
    decoder = BinaryDecoder(BytesIO(data))
    reader = DatumReader(writer_schema, reader_schema)
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

    assert deserialize(data, writer_schema=schema_v1, reader_schema=schema_v1) == {
        "name": "Alyssa",
        "favorite_number": 256,
    }


def test_deserialize_v1_v2():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
        },
        schema_v1,
    )

    assert deserialize(data, writer_schema=schema_v1, reader_schema=schema_v2) == {
        "name": "Alyssa",
        "favorite_number": 256,
        "favorite_color": "green",
    }


def test_deserialize_v2_v1():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
            "favorite_color": "red",
        },
        schema_v2,
    )

    assert deserialize(data, writer_schema=schema_v2, reader_schema=schema_v1) == {
        "name": "Alyssa",
        "favorite_number": 256,
    }
