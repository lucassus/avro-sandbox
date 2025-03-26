from fastavro.write import writer
from fastavro.read import reader
from fastavro.schema import load_schema

if __name__ == "__main__":
    schema_v1 = load_schema("user_v1.avsc")
    schema_v2 = load_schema("user_v2.avsc")

    with open("users.avro", "wb") as out:
        writer(
            out,
            schema_v1,
            [
                {"name": "Alyssa", "favorite_number": 256},
                {"name": "Ben", "favorite_number": 7, "favorite_color": "red"},
            ],
        )

    with open("users.avro", "rb") as fo:
        for record in reader(fo, schema_v2):
            print(record)
