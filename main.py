from fastavro.write import writer
from fastavro.read import reader
from fastavro.schema import load_schema

if __name__ == "__main__":
    schema = load_schema("user.avsc")

    with open("users.avro", "wb") as out:
        writer(
            out,
            schema,
            [
                {"name": "Alyssa", "favorite_number": 256},
                {"name": "Ben", "favorite_number": 7, "favorite_color": "red"},
            ],
        )

    with open("users.avro", "rb") as fo:
        for record in reader(fo):
            print(record)
