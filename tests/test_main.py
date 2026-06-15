import unittest
from unittest.mock import patch

from starhunt.main import init_conn


class InitConnTest(unittest.TestCase):
    @patch("starhunt.main.psycopg.connect")
    def test_reads_connection_arguments_from_environment(self, connect):
        environment = {
            "POSTGRES_HOST": "db.example.test",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "starhunt",
            "POSTGRES_USER": "starhunt-user",
            "POSTGRES_PASSWORD": "secret",
        }

        with patch.dict("os.environ", environment, clear=True):
            connection = init_conn()

        self.assertIs(connection, connect.return_value)
        connect.assert_called_once_with(
            host="db.example.test",
            port=5433,
            dbname="starhunt",
            user="starhunt-user",
            password="secret",
        )


if __name__ == "__main__":
    unittest.main()
