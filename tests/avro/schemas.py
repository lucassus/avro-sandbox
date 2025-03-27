import json

import avro.schema

schema_v1 = avro.schema.parse(
    json.dumps(
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
)

schema_v2 = avro.schema.parse(
    json.dumps(
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
)
