from cassandra.cluster import Cluster
from dotenv import load_dotenv
import os

# Cargar variables del archivo .env
load_dotenv()

# Leer host y puerto desde el entorno
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))

try:
    # Crear conexión con Cassandra
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()

    print("✅ Conexión exitosa a Cassandra. Keyspaces disponibles:\n")
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    for row in rows:
        print("•", row.keyspace_name)

    # Cerrar la conexión
    cluster.shutdown()

except Exception as e:
    print("❌ Error al conectar con Cassandra:")
    print(e)

