from flask import Flask
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import os
import uuid

load_dotenv()

ASTRA_DB_BUNDLE = os.getenv("ASTRA_DB_BUNDLE")
ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")

app = Flask(__name__)

def connect_to_astra():
    cloud_config = {
        'secure_connect_bundle': f"./{ASTRA_DB_BUNDLE}"
    }
    auth_provider = PlainTextAuthProvider("token", os.getenv("ASTRA_DB_PASSWORD"))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(ASTRA_DB_KEYSPACE)
    return session

@app.route("/")
def index():
    try:
        session = connect_to_astra()

        session.execute("""
            CREATE TABLE IF NOT EXISTS estudiantes (
                id UUID PRIMARY KEY,
                nombre TEXT,
                curso TEXT
            );
        """)

        session.execute("""
            INSERT INTO estudiantes (id, nombre, curso)
            VALUES (%s, %s, %s)
        """, (uuid.uuid4(), 'Rafa', 'IA y Big Data'))

        rows = session.execute("SELECT nombre, curso FROM estudiantes")
        html = "<h2>👨‍🎓 Lista de estudiantes:</h2><ul>"
        for row in rows:
            html += f"<li>{row.nombre} – {row.curso}</li>"
        html += "</ul>"

        return html

    except Exception as e:
        return f"<b>Error conectando a Cassandra:</b><br>{e}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)



