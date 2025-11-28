# schema/auth_schema.py
from pydantic import BaseModel

class LoginSchema(BaseModel):
    usuario: str
    contrasenia: str