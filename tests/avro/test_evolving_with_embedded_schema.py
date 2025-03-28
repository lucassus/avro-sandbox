import json
from io import BytesIO

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from tests.schemas import raw_schema_v1, raw_schema_v2

schema_v1 = avro.schema.parse(json.dumps(raw_schema_v1))

schema_v2 = avro.schema.parse(json.dumps(raw_schema_v2))


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    writer = DataFileWriter(stream, DatumWriter(), schema)
    writer.append(data)
    writer.flush()
    serialized = stream.getvalue()
    writer.close()
    return serialized


def deserialize(data: bytes, *, reader_schema) -> dict:
    stream = BytesIO(data)
    reader = DataFileReader(stream, DatumReader(readers_schema=reader_schema))
    return next(reader)


def test_deserialize_v1_v1():
    data = serialize(
        {
            "name": "Alyssa",
            "favorite_number": 256,
        },
        schema_v1,
    )
    assert len(data) == 240

    assert deserialize(data, reader_schema=schema_v1) == {
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

    assert deserialize(data, reader_schema=schema_v2) == {
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

    assert deserialize(data, reader_schema=schema_v1) == {
        "name": "Alyssa",
        "favorite_number": 256,
    }
