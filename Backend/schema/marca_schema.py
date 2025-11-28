from pydantic import BaseModel
from typing import Optional

class MarcaSchema(BaseModel):
    id_marca: Optional[int] = None
    nombre_marca: str