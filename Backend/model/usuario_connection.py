# model/usuario_connection.py
import psycopg
import os

class UsuarioConnection():
    conn = None

    def __init__(self):
        try:
            # Mantiene la conexión segura de producción en Render
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (Producción)")
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

    # READ - ACTUALIZADO: Ahora ordena de forma descendente (DESC) según tu local
    def read_usuario(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, usuario, nombre, apellido, correo, contrasenia 
                FROM usuario
                ORDER BY id DESC
                """
            )
            return cur.fetchall()

    def filtrar_usuario(self, id):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, usuario, nombre, apellido, correo, contrasenia 
                FROM usuario WHERE id = %s
                """, (id,)
            )
            return cur.fetchone()

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

    # R (usuario ÚNICO) - ACTUALIZADO: Ahora usa LOWER para evitar duplicados por mayúsculas
    def get_by_usuario(self, usuario: str):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, usuario, nombre, apellido, correo, contrasenia
                FROM usuario
                WHERE LOWER(usuario) = LOWER(%s)
                """, (usuario,)
            )
            return cur.fetchone()

    # R (correo ÚNICO) - NUEVO: Agregado desde tu versión local para validación de registros
    def get_by_correo(self, correo: str):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, usuario, nombre, apellido, correo, contrasenia
                FROM usuario
                WHERE LOWER(correo) = LOWER(%s)
                """, (correo,)
            )
            return cur.fetchone()

    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass