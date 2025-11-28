# schema/cliente_schema.py
from typing import Optional, Annotated
from pydantic import BaseModel, StringConstraints

RucStr = Annotated[str, StringConstraints(pattern=r"^\d{11}$", min_length=11, max_length=11)]

class ClienteSchema(BaseModel):
    id_cliente: Optional[int] = None
    nombre_empresa: str
    ruc: RucStr
    direccion: str