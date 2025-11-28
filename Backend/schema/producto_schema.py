from pydantic import BaseModel, conint, condecimal
from typing import Optional
from decimal import Decimal

class ProductoSchema(BaseModel):
    id_producto: Optional[int] = None
    nombre_producto: str
    id_marca: int
    precio_unitario: condecimal(max_digits=12, decimal_places=2)
    stock: conint(ge=0) = 0

    class Config:
        json_encoders = {Decimal: lambda v: float(v)}