raw_schema_v1 = {
    "namespace": "sandbox",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number", "type": ["int", "null"]},
    ],
}

raw_schema_v2 = {
    **raw_schema_v1,
    "fields": [
        *raw_schema_v1["fields"],
        {"name": "favorite_color", "type": "string", "default": "green"},
    ],
}
