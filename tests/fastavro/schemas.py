from fastavro.schema import parse_schema

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
