from fastavro.read import reader, schemaless_reader
from fastavro.schema import load_schema
from fastavro.write import schemaless_writer, writer

from models import User

if __name__ == "__main__":
    schema = load_schema("schemas/sandbox.User.avsc")

    records = [
        {"name": "Alyssa", "favorite_number": 256},
        {"name": "Ben", "favorite_number": 7, "favorite_color": "red"},
        {
            "name": "Anna",
            "favorite_number": 3,
            "primary_address": {
                "street": "123 Main St",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94105",
            },
        },
    ]

    with open("users.avro", "wb") as out:
        writer(out, schema, records)

    with open("users_schemaless.avro", "wb") as out:
        for record in records:
            schemaless_writer(out, schema, record)

    with open("users_schemaless.avro", "rb") as fo:
        for data in schemaless_reader(fo, schema):
            print(data)
            # record = User.model_validate(data)
            # print(record)
