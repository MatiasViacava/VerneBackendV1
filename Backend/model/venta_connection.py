# model/venta_connection.py
import psycopg
import os
from decimal import Decimal

class VentaConnection:
    conn = None

    def __init__(self):
        try:
            self.conn = psycopg.connect(os.getenv("DATABASE_URL"))
            print("✅ Conectado a PostgreSQL en Render (desde local)")
        except Exception as err:
            print(f"❌ Error conectando a la base de datos: {err}")

    # ----------------- helpers -----------------
    def _calc_importe(self, id_producto: int, cantidad: int) -> Decimal:
        """
        Obtiene el precio del producto y calcula el importe = precio * cantidad.
        Se castea a numeric para evitar problemas si usas MONEY en la DB.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT precio_unitario::numeric FROM producto WHERE id_producto = %s",
                (id_producto,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("Producto no encontrado")
            precio = Decimal(str(row[0]))
            return (precio * Decimal(cantidad)).quantize(Decimal("0.01"))

    # ----------------- CRUD -----------------
    def insert_venta(self, data: dict):
        """
        data = { id_producto, id_cliente, fecha(opc), cantidad, estado(opc) }
        """
        importe = self._calc_importe(data["id_producto"], data["cantidad"])

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO venta (id_producto, id_cliente, fecha, cantidad, importe_total, estado)
                VALUES (%(id_producto)s, %(id_cliente)s, COALESCE(%(fecha)s, CURRENT_DATE),
                        %(cantidad)s, %(importe_total)s, COALESCE(%(estado)s, 1))
                """,
                {
                    **data,
                    "importe_total": importe,
                },
            )
            self.conn.commit()

    def read_venta(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_venta, id_producto, id_cliente, fecha,
                       cantidad, importe_total, estado
                FROM venta
                ORDER BY id_venta ASC
                """
            )
            return cur.fetchall()

    def filtrar_venta(self, id_venta: int):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_venta, id_producto, id_cliente, fecha,
                       cantidad, importe_total, estado
                FROM venta
                WHERE id_venta = %s
                """,
                (id_venta,),
            )
            return cur.fetchone()

    def delete_venta(self, id_venta: int):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM venta WHERE id_venta = %s", (id_venta,))
            self.conn.commit()

    def update_venta(self, data: dict):
        """
        data = { id_venta, id_producto, id_cliente, fecha(opc), cantidad, estado(opc) }
        Siempre recalculamos el importe con los valores actuales.
        """
        importe = self._calc_importe(data["id_producto"], data["cantidad"])

        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE venta
                SET id_producto  = %(id_producto)s,
                    id_cliente   = %(id_cliente)s,
                    fecha        = COALESCE(%(fecha)s, fecha),
                    cantidad     = %(cantidad)s,
                    importe_total= %(importe_total)s,
                    estado       = COALESCE(%(estado)s, estado)
                WHERE id_venta   = %(id_venta)s
                """,
                {
                    **data,
                    "importe_total": importe,
                },
            )
            self.conn.commit()

    # ----------------- DASHBOARD -----------------
    def read_venta_view(self):
        """
        Devuelve ventas con nombres de cliente y producto.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    v.id_venta,
                    v.id_producto,
                    p.nombre_producto,
                    v.id_cliente,
                    c.nombre_empresa AS cliente_nombre,
                    v.fecha,
                    v.cantidad,
                    v.importe_total,
                    v.estado
                FROM venta v
                JOIN producto p ON p.id_producto = v.id_producto
                JOIN cliente  c ON c.id_cliente  = v.id_cliente
                ORDER BY v.fecha DESC, v.id_venta DESC
                """
            )
            return cur.fetchall()

    def __del__(self):
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
