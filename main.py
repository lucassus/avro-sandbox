from fastavro.read import reader
from fastavro.schema import load_schema
from fastavro.write import writer

if __name__ == "__main__":
    schema = load_schema("schemas/sandbox.User.avsc")

    with open("users.avro", "wb") as out:
        writer(
            out,
            schema,
            [
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
            ],
        )

    with open("users.avro", "rb") as fo:
        for record in reader(fo, schema):
            print(record)
