import psycopg
import os
class UsuarioConnection():
    conn = None
    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")

    def insert_usuario(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO usuario (usuario, nombre, apellido, correo, contrasenia)
                VALUES (%(usuario)s, %(nombre)s, %(apellido)s, %(correo)s, %(contrasenia)s)
                """, data
            )
            self.conn.commit()

    def read_usuario(self):
        with self.conn.cursor() as cur:
            data = cur.execute(
            """
            SELECT * FROM usuario
            """
            )
            return data.fetchall()

    def filtrar_usuario(self, id):
        with self.conn.cursor() as cur:
            data = cur.execute(
            """
            SELECT * FROM usuario WHERE id = %s
            """, (id,)
            )
            return data.fetchone()

    def delete_usuario(self, id):
        with self.conn.cursor() as cur:
            cur.execute(
            """
            DELETE FROM usuario WHERE id = %s
            """, (id,)
            )
            self.conn.commit()        
    
    def update_usuario(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
            """
            UPDATE usuario
            SET usuario = %(usuario)s,
                nombre = %(nombre)s,
                apellido = %(apellido)s,
                correo = %(correo)s,
                contrasenia = %(contrasenia)s
            WHERE id = %(id)s
            """, data
            )
            self.conn.commit()

    def get_by_usuario(self, usuario: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, usuario, nombre, apellido, correo, contrasenia
                FROM usuario
                WHERE usuario = %s
                """, (usuario,))
            return cur.fetchone()


    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass