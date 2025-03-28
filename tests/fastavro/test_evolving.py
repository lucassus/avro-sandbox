from io import BytesIO

from fastavro.read import schemaless_reader
from fastavro.schema import parse_schema
from fastavro.write import schemaless_writer

from tests.schemas import raw_schema_v1, raw_schema_v2

schema_v1 = parse_schema(raw_schema_v1)

schema_v2 = parse_schema(raw_schema_v2)


def serialize(data: dict, schema) -> bytes:
    stream = BytesIO()
    schemaless_writer(stream, schema, data)
    stream.seek(0)
    return stream.read()


def deserialize(data: bytes, *, writer_schema, reader_schema) -> dict:
    stream = BytesIO(data)
    return schemaless_reader(
        stream,
        writer_schema=writer_schema,
        reader_schema=reader_schema,
    )


def test_deserialize_v1_v1():
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
