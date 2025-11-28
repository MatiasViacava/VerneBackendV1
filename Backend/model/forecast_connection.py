# model/forecast_connection.py
import psycopg
import os

class ForecastConnection:
    conn = None
    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")
    # --------- INSERT CABECERA ---------
    def insert_run(self, data):
        """
        Inserta una corrida en forecast_run y devuelve id_run.
        Hace rollback si algo falla para no dejar la transacción en estado abortado.
        """
        if not self.conn:
            raise RuntimeError("No hay conexión a la BD (forecast_run).")

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO forecast_run
                        (id_usuario, origen, modelo, modelo_version,
                         periodo_inicio, periodo_fin, horizonte_meses)
                    VALUES
                        (%(id_usuario)s, %(origen)s, %(modelo)s, %(modelo_version)s,
                         %(periodo_inicio)s, %(periodo_fin)s, %(horizonte_meses)s)
                    RETURNING id_run
                    """,
                    data,
                )
                id_run = cur.fetchone()[0]
            self.conn.commit()
            return id_run
        except Exception as e:
            # súper importante: limpiar la transacción
            self.conn.rollback()
            print("Error insert_run:", e)
            raise

    # --------- INSERT DETALLE (MANY) ---------
    def insert_detalle_many(self, detalles):
        """
        Inserta muchas filas en forecast_detalle.
        """
        if not detalles:
            return
        if not self.conn:
            raise RuntimeError("No hay conexión a la BD (forecast_detalle).")

        try:
            with self.conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO forecast_detalle
                        (id_run, id_producto, fecha_mes,
                         venta_predicha, baseline,
                         categoria_abc, categoria_xyz, categoria_abcxyz)
                    VALUES
                        (%(id_run)s, %(id_producto)s, %(fecha_mes)s,
                         %(venta_predicha)s, %(baseline)s,
                         %(categoria_abc)s, %(categoria_xyz)s, %(categoria_abcxyz)s)
                    """,
                    detalles,
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print("Error insert_detalle_many:", e)
            raise

    # --------- SELECT CABECERAS ---------
    def read_runs(self):
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_run,
                       creado_en,
                       origen,
                       modelo,
                       modelo_version,
                       periodo_inicio,
                       periodo_fin,
                       horizonte_meses
                FROM forecast_run
                ORDER BY id_run DESC
                """
            )
            return cur.fetchall()

    # --------- SELECT DETALLE POR RUN ---------
    def read_detalle_by_run(self, id_run: int):
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.id_detalle,
                       d.id_run,
                       d.id_producto,
                       p.nombre_producto,
                       d.fecha_mes,
                       d.venta_predicha,
                       d.baseline,
                       d.categoria_abc,
                       d.categoria_xyz,
                       d.categoria_abcxyz
                FROM forecast_detalle d
                LEFT JOIN producto p ON p.id_producto = d.id_producto
                WHERE d.id_run = %s
                ORDER BY d.fecha_mes, p.nombre_producto
                """,
                (id_run,),
            )
            return cur.fetchall()

    # --------- DELETE RUN COMPLETO ---------
    def delete_run(self, id_run: int):
        """
        Elimina forecast_detalle + forecast_run para un id_run.
        """
        if not self.conn:
            raise RuntimeError("No hay conexión a la BD (delete_run).")

        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM forecast_detalle WHERE id_run = %s", (id_run,))
                cur.execute("DELETE FROM forecast_run WHERE id_run = %s", (id_run,))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print("Error delete_run:", e)
            raise

    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
