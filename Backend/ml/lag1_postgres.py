# ml/lag1_postgres.py
from datetime import date

from model.venta_connection import VentaConnection
from .runtime_xgb import GLOBAL_MEAN
from .date_utils import first_day_of_month, prev_month_start


venta_conn = VentaConnection()


def lag1_from_postgres(id_producto: int, fecha_mes: date) -> float:
    """
    Calcula el baseline tipo lag_1 para (id_producto, fecha_mes) a partir de la tabla 'venta',
    usando **importe_total** como medida de venta mensual.

    Lógica:
      1) Ventas (importe_total) del mes anterior (agregado por mes)
      2) Si no existe ese mes, toma la última venta mensual anterior
      3) Si no existe historial previo, usa la media mensual del producto (importe_total)
      4) Si el producto nunca vendió, usa la media mensual global (importe_total)

    Devuelve siempre un float (si todo falla, GLOBAL_MEAN).
    """
    month_start = first_day_of_month(fecha_mes)
    prev_start = prev_month_start(month_start)

    conn = venta_conn.conn
    if conn is None:
        # si por algún motivo no se conectó, devolvemos GLOBAL_MEAN
        return float(GLOBAL_MEAN)

    with conn.cursor() as cur:
        # 1) venta (importe_total) del mes anterior
        cur.execute(
            """
            SELECT SUM(importe_total)::float AS venta_mensual
            FROM venta
            WHERE id_producto = %s
              AND estado = 1
              AND date_trunc('month', fecha) = date_trunc('month', %s::date);
            """,
            (id_producto, prev_start),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])

        # 2) última venta mensual anterior a 'fecha_mes'
        cur.execute(
            """
            SELECT SUM(importe_total)::float AS venta_mensual
            FROM venta
            WHERE id_producto = %s
              AND estado = 1
              AND fecha < %s::date
            GROUP BY date_trunc('month', fecha)
            ORDER BY date_trunc('month', fecha) DESC
            LIMIT 1;
            """,
            (id_producto, month_start),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])

        # 3) media mensual del producto (en importe_total)
        cur.execute(
            """
            SELECT AVG(mensual)::float
            FROM (
              SELECT SUM(importe_total) AS mensual
              FROM venta
              WHERE id_producto = %s
                AND estado = 1
              GROUP BY date_trunc('month', fecha)
            ) t;
            """,
            (id_producto,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])

        # 4) media mensual global (en importe_total)
        cur.execute(
            """
            SELECT AVG(mensual)::float
            FROM (
              SELECT SUM(importe_total) AS mensual
              FROM venta
              WHERE estado = 1
              GROUP BY id_producto, date_trunc('month', fecha)
            ) t;
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])

    # si absolutamente todo falla, usamos el GLOBAL_MEAN del training
    return float(GLOBAL_MEAN)
