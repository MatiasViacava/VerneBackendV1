from pydantic import BaseModel
from typing import Optional

class TipoUsuarioSchema(BaseModel):
    id_tipousuario: Optional[int] = None
    tipo_usuario: str
    id_usuario: int