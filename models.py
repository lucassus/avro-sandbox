from typing import Optional

from pydantic import BaseModel


class Address(BaseModel):
    street: str
    city: str
    state: str
    zip: str


class User(BaseModel):
    name: str
    favorite_number: int
    favorite_color: Optional[str] = None
    primary_address: Optional[Address] = None
