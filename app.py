from flask import Flask, request
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import os
import uuid

load_dotenv()

ASTRA_DB_BUNDLE = os.getenv("ASTRA_DB_BUNDLE")
ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
ASTRA_DB_PASSWORD = os.getenv("ASTRA_DB_PASSWORD")

app = Flask(__name__)

def connect_to_astra():
    cloud_config = {
        'secure_connect_bundle': f"./{ASTRA_DB_BUNDLE}"
    }
    auth_provider = PlainTextAuthProvider("token", ASTRA_DB_PASSWORD)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(ASTRA_DB_KEYSPACE)
    return session

# ----------------------------------
# RUTA PRINCIPAL - MENÚ
# ----------------------------------
@app.route("/")
def index():
    return """
        <h1>🔗 API Flask - Cassandra Astra</h1>
        <p>Consultas disponibles:</p>
        <ul>
            <li><a href="/cliente_datos">Buscar datos del cliente por ID</a></li> 
            <li><a href="/pedidos_cliente_buscar">Buscar pedidos por cliente</a></li>
            <li><a href="/pedidos_fecha_buscar">Buscar pedidos por fecha</a></li>
            <li><a href="/productos_pedido_buscar">Buscar productos por pedido</a></li>
            <li><a href="/buscar">Buscar clientes_id por nombre</a></li>
            <li><a href="/clientes_visibles">Lista de clientes</a></li>
        </ul>
    """

# ----------------------------------
# CLIENTE: obtener datos por clientes_id
# ----------------------------------
@app.route("/cliente_datos", methods=["GET", "POST"])
def cliente_por_id():
    if request.method == "GET":
        return '''
            <h2>Buscar datos del cliente</h2>
            <form method="post">
                Cliente ID: <input type="text" name="clientes_id">
                <input type="submit" value="Buscar">
            </form>
        '''
    else:
        try:
            import uuid
            clientes_id = uuid.UUID(request.form.get("clientes_id"))
            session = connect_to_astra()
            result = session.execute("SELECT * FROM clientes WHERE clientes_id=%s", (clientes_id,))
            row = result.one()
            if row:
                return f"""
                    <h3>Datos del cliente:</h3>
                    <ul>
                        <li><b>ID:</b> {row.clientes_id}</li>
                        <li><b>Nombre:</b> {row.nombre}</li>
                        <li><b>Dirección:</b> {row.direccion}</li>
                        <li><b>Teléfono:</b> {row.numero_telefono}</li>
                    </ul>
                """
            else:
                return f"No se encontró ningún cliente con ID: {clientes_id}"
        except Exception as e:
            return f"<b>Error:</b> {e}"

# ----------------------------------
# CLIENTES_VISIBLES: buscar clientes_id por nombre
# ----------------------------------
@app.route("/buscar", methods=["GET", "POST"])
def buscar_cliente():
    if request.method == "GET":
        return '''
            <h2>Buscar clientes_id por nombre</h2>
            <form method="post">
                Nombre: <input type="text" name="nombre">
                <input type="submit" value="Buscar">
            </form>
        '''
    else:
        nombre = request.form.get("nombre")
        try:
            session = connect_to_astra()
            result = session.execute("SELECT clientes_id FROM clientes_visibles WHERE nombre=%s", (nombre,))
            row = result.one()
            if row:
                return f"<h3>Resultado:</h3><p>Cliente: <b>{nombre}</b><br>ID: {row.clientes_id}</p>"
            else:
                return f"No se encontró ningún cliente con nombre: <b>{nombre}</b>"
        except Exception as e:
            return f"<b>Error:</b> {e}"

# ----------------------------------
# PEDIDOS: por cliente (clientes_id)
# ----------------------------------
@app.route("/pedidos_cliente_buscar", methods=["GET", "POST"])
def pedidos_por_cliente():
    if request.method == "GET":
        return '''
            <h2>Buscar pedidos por cliente</h2>
            <form method="post">
                Cliente ID: <input type="text" name="clientes_id">
                <input type="submit" value="Buscar">
            </form>
        '''
    else:
        try:
            clientes_id = uuid.UUID(request.form.get("clientes_id"))
            session = connect_to_astra()
            query = "SELECT * FROM pedidos_por_cliente WHERE clientes_id=%s"
            rows = session.execute(query, (clientes_id,))
            resultado = "<h3>Pedidos encontrados:</h3><ul>"
            for row in rows:
                resultado += f"<li>Pedido: {row.pedido_id}, Estado: {row.estado}, Fecha: {row.fecha}</li>"
            resultado += "</ul>"
            return resultado or "No se encontraron pedidos para este cliente."
        except Exception as e:
            return f"<b>Error:</b> {e}"

# ----------------------------------
# PEDIDOS: por fecha (YYYY-MM-DD)
# ----------------------------------
@app.route("/pedidos_fecha_buscar", methods=["GET", "POST"])
def pedidos_por_fecha():
    if request.method == "GET":
        return '''
            <h2>Buscar pedidos por fecha</h2>
            <form method="post">
                Fecha (YYYY-MM-DD): <input type="text" name="fecha">
                <input type="submit" value="Buscar">
            </form>
        '''
    else:
        fecha = request.form.get("fecha")
        try:
            session = connect_to_astra()
            query = "SELECT * FROM pedidos_por_fecha WHERE fecha=%s"
            rows = session.execute(query, (fecha,))
            resultado = "<h3>Pedidos encontrados:</h3><ul>"
            for row in rows:
                resultado += f"<li>Pedido: {row.pedido_id}, Estado: {row.estado}</li>"
            resultado += "</ul>"
            return resultado or "No se encontraron pedidos en esa fecha."
        except Exception as e:
            return f"<b>Error:</b> {e}"

# ----------------------------------
# PRODUCTOS: por pedido_id
# ----------------------------------
@app.route("/productos_pedido_buscar", methods=["GET", "POST"])
def productos_por_pedido():
    if request.method == "GET":
        return '''
            <h2>Buscar productos por pedido</h2>
            <form method="post">
                Pedido ID: <input type="text" name="pedido_id">
                <input type="submit" value="Buscar">
            </form>
        '''
    else:
        try:
            pedido_id = uuid.UUID(request.form.get("pedido_id"))
            session = connect_to_astra()
            query = "SELECT * FROM productos_por_pedido WHERE pedido_id=%s"
            rows = session.execute(query, (pedido_id,))
            resultado = "<h3>Productos encontrados:</h3><ul>"
            for row in rows:
                productos = ", ".join(row.lista_productos) if row.lista_productos else "Sin productos"
                resultado += f"<li>Fecha: {row.fecha}, Estado: {row.estado}, Productos: {productos}</li>"
            resultado += "</ul>"
            return resultado or "No se encontraron productos para ese pedido."
        except Exception as e:
            return f"<b>Error:</b> {e}"

# ----------------------------------
# CLIENTE: Lista clientes visibles
# ----------------------------------
@app.route("/clientes_visibles")
def listar_clientes_visibles():
    try:
        session = connect_to_astra()
        rows = session.execute("SELECT nombre, clientes_id FROM clientes_visibles")
        html = "<h2>Clientes visibles:</h2><ul>"
        for row in rows:
            html += f"<li>{row.nombre} → {row.clientes_id}</li>"
        html += "</ul>"
        return html
    except Exception as e:
        return f"<b>Error:</b> {e}"

# ----------------------------------
# RUN FLASK
# ----------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

