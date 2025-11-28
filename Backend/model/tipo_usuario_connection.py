# model/tipo_usuario_connection.py
import psycopg
import os
class TipoUsuarioConnection:
    conn = None

    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")

    def insert_tipo_usuario(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tipo_usuario (tipo_usuario, id_usuario)
                VALUES (%(tipo_usuario)s, %(id_usuario)s)
                """,
                data,
            )
            self.conn.commit()

    def read_tipo_usuario(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_tipousuario, tipo_usuario, id_usuario
                FROM tipo_usuario
                ORDER BY id_tipousuario
                """
            )
            return cur.fetchall()

    def filtrar_tipo_usuario(self, id_tipousuario):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_tipousuario, tipo_usuario, id_usuario
                FROM tipo_usuario
                WHERE id_tipousuario = %s
                """,
                (id_tipousuario,),
            )
            return cur.fetchone()

    def listar_por_usuario(self, id_usuario):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_tipousuario, tipo_usuario, id_usuario
                FROM tipo_usuario
                WHERE id_usuario = %s
                ORDER BY id_tipousuario
                """,
                (id_usuario,),
            )
            return cur.fetchall()

    def delete_tipo_usuario(self, id_tipousuario):
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tipo_usuario WHERE id_tipousuario = %s",
                (id_tipousuario,),
            )
            self.conn.commit()

    def update_tipo_usuario(self, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tipo_usuario
                   SET tipo_usuario = %(tipo_usuario)s,
                       id_usuario   = %(id_usuario)s
                 WHERE id_tipousuario = %(id_tipousuario)s
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
