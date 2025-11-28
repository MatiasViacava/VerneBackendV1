# schema/venta_schema.py
from pydantic import BaseModel, conint
from typing import Optional
from datetime import date

class VentaSchema(BaseModel):
    id_venta: Optional[int] = None
    id_producto: int
    id_cliente: int
    fecha: Optional[date] = None           # si no viene, usamos CURRENT_DATE en SQL
    cantidad: conint(gt=0)                 # > 0
    estado: Optional[int] = 1              # 1=activa, 0=anulada, etc.
