# schema/abcxyz_schema.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime
import json

from pathlib import Path
CONFIG_PATH = Path(__file__).with_name("abcxyz_config.json")

class ABCXYZConfigSchema(BaseModel):
    a_cut: float = Field(0.80, ge=0.0, le=1.0, description="Umbral acumulado para A (ej. 0.80)")
    b_cut: float = Field(0.95, ge=0.0, le=1.0, description="Umbral acumulado para B (ej. 0.95)")
    x_cut: float = Field(0.50, ge=0.0,  description="CV límite para X")
    y_cut: float = Field(0.90, ge=0.0,  description="CV límite para Y")

def load_config() -> ABCXYZConfigSchema:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return ABCXYZConfigSchema(**data)
    except Exception:
        return ABCXYZConfigSchema()

def save_config(cfg: ABCXYZConfigSchema):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg.dict(), f, ensure_ascii=False, indent=2)

def last_12_month_keys(today: Optional[date] = None) -> List[str]:
    if today is None:
        today = date.today()
    y, m = today.year, today.month
    keys = []
    for i in range(11, -1, -1):
        mm = m - i
        yy = y
        while mm <= 0:
            mm += 12
            yy -= 1
        keys.append(f"{yy}-{mm:02d}")
    return keys

def month_key_from_date(d: date | datetime) -> str:
    return f"{d.year}-{d.month:02d}"

def abc_label_from_cumshare(cum_share: float, a_cut: float, b_cut: float) -> str:
    if cum_share <= a_cut:
        return "A"
    elif cum_share <= b_cut:
        return "B"
    return "C"

def xyz_label_from_cv(cv: float, x_cut: float, y_cut: float) -> str:
    if cv <= x_cut:
        return "X"
    elif cv <= y_cut:
        return "Y"
    return "Z"
