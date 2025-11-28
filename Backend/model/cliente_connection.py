# model/cliente_connection.py
import psycopg
import os

class ClienteConnection:
    conn = None

    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")

    # C
    def insert_cliente(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cliente (nombre_empresa, ruc, direccion)
                VALUES (%(nombre_empresa)s, %(ruc)s, %(direccion)s)
                """,
                data,
            )
            self.conn.commit()

    # R (listar)
    def read_cliente(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id_cliente, nombre_empresa, ruc, direccion FROM cliente ORDER BY id_cliente")
            return cur.fetchall()

    # R (uno)
    def filtrar_cliente(self, id_cliente):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id_cliente, nombre_empresa, ruc, direccion FROM cliente WHERE id_cliente = %s",
                (id_cliente,),
            )
            return cur.fetchone()

    # D
    def delete_cliente(self, id_cliente):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM cliente WHERE id_cliente = %s", (id_cliente,))
            self.conn.commit()

    # U
    def update_cliente(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cliente
                SET nombre_empresa = %(nombre_empresa)s,
                    ruc = %(ruc)s,
                    direccion = %(direccion)s
                WHERE id_cliente = %(id_cliente)s
                """,
                data,
            )
            self.conn.commit()

    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
