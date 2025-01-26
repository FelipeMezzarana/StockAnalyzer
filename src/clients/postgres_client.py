# Third party
import psycopg2

# Local
from ..abstract.client import Client
from ..settings import Settings
from ..utils.constants import PIPELINES
from ..utils.decorators import singleton
from ..utils.get_logger import get_logger


@singleton
class PostgresClient(Client):
    """Postgres Client."""

    def __init__(self, settings: Settings):
        """Connect with DB."""

        self.logger = get_logger(__name__, settings)
        self.settings = settings
        config = settings.CLIENT_CONFIG
        uid = config["POSTGRES_USER"]
        pwd = config["POSTGRES_PASSWORD"]
        host = config["POSTGRES_HOST"]
        db = config["POSTGRES_DB"]
        self.conn_string = f"host={host} dbname={db} user={uid} password={pwd}"
        self.connect()
        for schema in PIPELINES.schemas:
            self._create_schema(schema)

    def connect(self):
        """Connect to db."""
        self.conn = psycopg2.connect(self.conn_string)
        self.cur = self.conn.cursor()
        self.logger.info("conected")

    def execute(self, query: str) -> list[tuple]:
        """Execute query."""

        self.cur.execute(query)
        self.conn.commit()
        if self.cur.rowcount > 0:
            resp = self.cur.fetchall()
        else:
            resp = []

        return resp

    def executemany(self, query: str, mapped_values: list[tuple]):
        """Execute parameterized query."""
        self.cur.executemany(query, mapped_values)
        self.conn.commit()

    def _schema_exist(self, schema_name: str) -> bool:
        """Check if schemas exists."""
        query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM   information_schema.schemata
            WHERE  schema_name = '{schema_name}'
        );
        """
        schema_exist = self.execute(query)

        return schema_exist[0][0]

    def _create_schema(self, schema_name: str):
        """Create schema."""

        if not self._schema_exist(schema_name):
            query = f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name};
            """
            self.execute(query)
            self.logger.info(f"Created schema {schema_name}")
        else:
            self.logger.info(f"Schema {schema_name} already exists")
