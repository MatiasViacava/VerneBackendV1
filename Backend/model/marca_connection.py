# model/marca_connection.py
import psycopg
import os
class MarcaConnection:
    conn = None

    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")

    def read_marca(self):
        with self.conn.cursor() as cur:
            data = cur.execute("""
                SELECT id_marca, nombre_marca
                FROM marca
                ORDER BY nombre_marca ASC
            """)
            return data.fetchall()

    def filtrar_marca(self, id_marca: int):
        with self.conn.cursor() as cur:
            data = cur.execute("""
                SELECT id_marca, nombre_marca
                FROM marca
                WHERE id_marca = %s
            """, (id_marca,))
            return data.fetchone()

    def insert_marca(self, data: dict):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO marca (nombre_marca)
                VALUES (%(nombre_marca)s)
            """, data)
            self.conn.commit()

    def update_marca(self, data: dict):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE marca
                SET nombre_marca = %(nombre_marca)s
                WHERE id_marca = %(id_marca)s
            """, data)
            self.conn.commit()

    def delete_marca(self, id_marca: int):
        with self.conn.cursor() as cur:
            cur.execute("""
                DELETE FROM marca WHERE id_marca = %s
            """, (id_marca,))
            self.conn.commit()

    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
