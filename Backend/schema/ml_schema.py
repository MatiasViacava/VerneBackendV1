# schema/ml_schema.py
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class ForecastItem(BaseModel):
    id_producto: int = Field(
        ...,
        description="ID de la tabla producto (o código del CSV)"
    )
    producto: str
    marca: str
    fecha_mes: date
    pct_chg_1: float | None = 0.0

    # Etiquetas de clasificación en el momento de la predicción
    categoria_abc: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=1,
        description="Etiqueta ABC (A/B/C) usada para esta predicción",
    )
    categoria_xyz: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=1,
        description="Etiqueta XYZ (X/Y/Z) usada para esta predicción",
    )


class ForecastRequest(BaseModel):
    """
    origen:
      - 'abcxyz_db'  -> selección de productos hecha desde la BD
      - 'abcxyz_csv' -> selección de productos hecha desde un archivo CSV/XLSX
    """
    origen: Optional[str] = Field(default="abcxyz_db")
    items: List[ForecastItem]


class ForecastResponseItem(BaseModel):
    id_producto: int
    producto: str
    fecha_mes: date
    prediccion: float


class ForecastRunInfo(BaseModel):
    id_run: int
    creado_en: datetime
    origen: str
    modelo: str
    modelo_version: str
    periodo_inicio: date
    periodo_fin: date
    horizonte_meses: int


class ForecastHistoryDetail(BaseModel):
    id_detalle: int
    id_run: int

    # AHORA OPCIONALES -> permiten NULL en corridas que vienen de CSV
    id_producto: Optional[int] = None
    producto: Optional[str] = None

    fecha_mes: date
    venta_predicha: float
    baseline: float
    categoria_abc: Optional[str] = None
    categoria_xyz: Optional[str] = None
    categoria_abcxyz: Optional[str] = None
