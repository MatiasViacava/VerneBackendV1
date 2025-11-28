from pydantic import BaseModel
from typing import Optional

class UsuarioSchema(BaseModel):
    id: Optional[int] = None
    usuario: str
    nombre: str
    apellido: str
    correo: str
    contrasenia: str

