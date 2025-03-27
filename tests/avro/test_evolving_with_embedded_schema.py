from io import BytesIO

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from tests.avro.schemas import schema_v1, schema_v2


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
    assert len(data) == 240

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
