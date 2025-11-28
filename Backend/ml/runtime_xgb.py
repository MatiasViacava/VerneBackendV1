# ml/runtime_xgb.py
from __future__ import annotations

from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Any, Union
import math
import json

import numpy as np
import pandas as pd
import joblib
from xgboost import XGBRegressor

# =========================
# Carga de artifacts
# =========================

BASE_DIR = Path(__file__).resolve().parent
ART_DIR  = BASE_DIR / "artifacts_xgb"

with open(ART_DIR / "feature_maps.json", "r", encoding="utf-8") as f:
    FMAPS = json.load(f)

with open(ART_DIR / "meta.json", "r", encoding="utf-8") as f:
    META = json.load(f)

GLOBAL_MEAN    = float(FMAPS["global_mean"])
KEY_MEAN_MAP   = FMAPS["key_mean_map"]      # producto -> media
KEY_MONTH_MAP  = FMAPS["key_month_map"]     # "producto:mes" -> media
MONTH_GLOB_MAP = FMAPS["month_glob_map"]    # "mes" -> media global
CAT_MEAN_MAPS  = FMAPS["cat_mean_maps"]     # p.ej. {"marca": {...}}
XTR_RAW_COLUMNS = FMAPS["Xtr_raw_columns"]  # orden de columnas crudas

DATE_COL = META.get("date_col", "fecha_mes")
KEY_COL  = META.get("key_col", "producto")
TARGET   = META.get("target_col", "venta_mensual")
GAMMA    = float(META.get("gamma", 1.0))
EPS      = 1e-3

# Preprocess + modelo
PREPROCESS = joblib.load(ART_DIR / "preprocess.pkl")

MODEL = XGBRegressor()
MODEL.load_model(str(ART_DIR / "xgb_model.json"))


# =========================
# Helpers de fechas
# =========================

def _to_timestamp(d: Union[str, date, datetime]) -> pd.Timestamp:
    if isinstance(d, pd.Timestamp):
        return d
    return pd.to_datetime(d)


def _month_sin_cos(month: int) -> tuple[float, float]:
    ang = 2 * math.pi * month / 12.0
    return math.sin(ang), math.cos(ang)


# =========================
# Baseline (lag_1)
# =========================
#
# En tu notebook el baseline es lag_1:
#   - si existe mes anterior -> usa esa venta_mensual
#   - si no, último mes previo
#   - si no, media del producto
#   - si no, media global
#
# En producción lo ideal es calcularlo desde PostgreSQL
# usando la tabla "venta" agregada por mes.
#
# Aquí dejo una versión genérica que solo usa los mapas como fallback.
# Tú puedes reemplazarla por una que consulte la BD.

def baseline_from_maps(producto: str, fecha_mes: Union[str, date, datetime]) -> float:
    """
    Baseline aproximado usando únicamente feature_maps.
    NO es exactamente lag_1, pero sigue la filosofía del notebook:
    se basa en medias por producto y por mes.
    """
    ts = _to_timestamp(fecha_mes)
    month = int(ts.month)

    # media por producto en train
    key_mean = float(KEY_MEAN_MAP.get(producto, GLOBAL_MEAN))

    # media producto+mes (si existe)
    key_month_key = f"{producto}:{month}"
    key_month_mean = float(KEY_MONTH_MAP.get(key_month_key, key_mean))

    # media global por mes
    month_mean = float(MONTH_GLOB_MAP.get(str(month), GLOBAL_MEAN))

    # puedes combinar como quieras; aquí tomo algo simple:
    #   baseline ≈ media_producto_mes ajustada por patrón global del mes
    # Ejemplo: if key_month_mean conocido, úsalo; si no, key_mean.
    baseline = key_month_mean

    # Podrías refinar:
    # baseline *= (month_mean / GLOBAL_MEAN)
    return baseline


# =========================
# Construcción de features
# =========================

def build_feature_row(
    *,
    producto: str,
    marca: str,
    fecha_mes: Union[str, date, datetime],
    pct_chg_1: float = 0.0,
) -> Dict[str, Any]:
    """
    Replica la estructura de features crudos que usa tu pipeline.
    Equivalente a la fila 'fila' que construyes en predecir_xgb_registro(...)
    en el notebook.
    """
    ts = _to_timestamp(fecha_mes)
    year = int(ts.year)
    month = int(ts.month)
    qtr = (month - 1) // 3 + 1

    month_sin, month_cos = _month_sin_cos(month)

    # medias de entrenamiento
    key_mean_train = float(KEY_MEAN_MAP.get(producto, GLOBAL_MEAN))

    key_month_key = f"{producto}:{month}"
    key_month_mean = float(KEY_MONTH_MAP.get(key_month_key, key_mean_train))

    month_glob_mean = float(MONTH_GLOB_MAP.get(str(month), GLOBAL_MEAN))

    # factores
    key_month_factor = (
        key_month_mean / key_mean_train if key_mean_train > 0 else 1.0
    )
    month_factor_glob = (
        month_glob_mean / GLOBAL_MEAN if GLOBAL_MEAN > 0 else 1.0
    )

    # target encoding por marca
    te_marca = float(
        CAT_MEAN_MAPS.get("marca", {}).get(marca, GLOBAL_MEAN)
    )

    # armar fila cruda con TODAS las columnas que usó Xtr_raw
    row = {
        "marca": marca,
        "producto": producto,
        "fecha_mes": ts,          # en tu DF original es datetime
        "pct_chg_1": float(pct_chg_1),
        "year": year,
        "month": month,
        "qtr": qtr,
        "key_mean_train": key_mean_train,
        "key_month_factor": key_month_factor,
        "month_factor_glob": month_factor_glob,
        "month_sin": month_sin,
        "month_cos": month_cos,
        "te_marca": te_marca,
    }

    # Asegurar orden correcto de columnas
    return {col: row[col] for col in XTR_RAW_COLUMNS}


# =========================
# Reconstrucción y predicción
# =========================

def recon(base: np.ndarray, rhat: np.ndarray, gamma: float = GAMMA, clip: float = 2.0) -> np.ndarray:
    """
    Misma fórmula que usas en el notebook:
    y_pred = (base + EPS) * exp( clip(gamma*rhat, -clip, clip) ) - EPS
    """
    base = np.asarray(base, dtype=float)
    rhat = np.asarray(rhat, dtype=float)
    return (base + EPS) * np.exp(np.clip(gamma * rhat, -clip, clip)) - EPS


def predict_batch(
    items: List[Dict[str, Any]],
    baselines: List[float],
) -> np.ndarray:
    """
    items: lista de filas crudas construidas con build_feature_row(...)
    baselines: baseline (lag_1 o equivalente) para cada fila
    """
    if not items:
        return np.array([], dtype=float)

    df_raw = pd.DataFrame(items)[XTR_RAW_COLUMNS]
    Xt = PREPROCESS.transform(df_raw)
    rhat = MODEL.predict(Xt)

    y_base = np.asarray(baselines, dtype=float)
    y_pred = recon(y_base, rhat, gamma=GAMMA, clip=2.0)

    # sin negativos
    return np.maximum(y_pred, 0.0)


def predict_one(
    producto: str,
    marca: str,
    fecha_mes: Union[str, date, datetime],
    pct_chg_1: float = 0.0,
    baseline: float | None = None,
) -> float:
    """
    Predicción para un solo producto-mes.

    Si no pasas baseline, usa baseline_from_maps (aproximado).
    Lo ideal en producción es que baseline sea lag_1 calculado desde PostgreSQL.
    """
    if baseline is None:
        baseline = baseline_from_maps(producto, fecha_mes)

    row = build_feature_row(
        producto=producto,
        marca=marca,
        fecha_mes=fecha_mes,
        pct_chg_1=pct_chg_1,
    )
    y_pred = predict_batch([row], [baseline])[0]
    return float(y_pred)