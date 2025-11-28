import psycopg
import os
class ProductoConnection:
    conn = None

    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")

    # CREATE
    def insert_producto(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO producto (nombre_producto, id_marca, precio_unitario, stock)
                VALUES (%(nombre_producto)s, %(id_marca)s, %(precio_unitario)s, %(stock)s)
                RETURNING id_producto
                """,
                data
            )
            new_id = cur.fetchone()[0]
            self.conn.commit()
            return new_id

    # READ (lista simple)
    def read_producto(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  p.id_producto,
                  p.nombre_producto,
                  p.id_marca,
                  -- si tu columna es MONEY, usa CAST:
                  -- (p.precio_unitario)::numeric AS precio_unitario,
                  p.precio_unitario AS precio_unitario,
                  p.stock
                FROM producto p
                ORDER BY p.id_producto
                """
            )
            return cur.fetchall()

    # READ (lista con nombre de marca)
    def read_producto_view(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  p.id_producto,
                  p.nombre_producto,
                  p.id_marca,
                  -- si es MONEY:
                  -- (p.precio_unitario)::numeric AS precio_unitario,
                  p.precio_unitario AS precio_unitario,
                  p.stock,
                  m.nombre_marca
                FROM producto p
                JOIN marca m ON m.id_marca = p.id_marca
                ORDER BY p.id_producto
                """
            )
            return cur.fetchall()

    # READ uno
    def filtrar_producto(self, id_producto):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  p.id_producto,
                  p.nombre_producto,
                  p.id_marca,
                  -- (p.precio_unitario)::numeric AS precio_unitario,
                  p.precio_unitario AS precio_unitario,
                  p.stock
                FROM producto p
                WHERE p.id_producto = %s
                """,
                (id_producto,)
            )
            return cur.fetchone()

    # DELETE
    def delete_producto(self, id_producto):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM producto WHERE id_producto = %s", (id_producto,))
            self.conn.commit()

    # UPDATE
    def update_producto(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE producto
                SET nombre_producto = %(nombre_producto)s,
                    id_marca        = %(id_marca)s,
                    precio_unitario = %(precio_unitario)s,
                    stock           = %(stock)s
                WHERE id_producto = %(id_producto)s
                """,
                data
            )
            self.conn.commit()

    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
