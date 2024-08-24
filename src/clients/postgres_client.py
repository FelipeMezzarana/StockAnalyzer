# Third party
import psycopg2

# Local
from ..abstract.client import Client
from ..settings import Settings
from ..util.get_logger import get_logger


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
