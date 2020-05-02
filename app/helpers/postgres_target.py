import datetime
import logging
import luigi
import re

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
    import psycopg2.extensions
except ImportError:
    logger.warning\
        ("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


class PostgresTarget(luigi.Target):
    """
    Target for a resource in Postgres.
    """
    marker_table = luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates')

    # if not supplied, fall back to default Postgres port
    DEFAULT_DB_PORT = 5432

    # Use DB side timestamps or client side timestamps in the marker_table
    use_db_timestamps = True

    def __init__(
        self, host, database, user, password, table, update_id, port=None
    ):
        """
        Args:
            host (str): Postgres server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set
            port (int): Postgres server port.
        """
        if ':' in host:
            self.host, self.port = host.split(':')
        else:
            self.host = host
            self.port = port or self.DEFAULT_DB_PORT
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

    def touch(self, connection=None):
        """
        Mark this update as complete.
        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        if self.use_db_timestamps:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table)
                   VALUES (%s, %s)
                """.format(marker_table=self.marker_table),
                (self.update_id, self.table))
        else:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table, inserted)
                         VALUES (%s, %s, %s);
                    """.format(marker_table=self.marker_table),
                (self.update_id, self.table,
                 datetime.datetime.now()))

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("""SELECT 1 FROM {marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(marker_table=self.marker_table),
                           (self.update_id,)
                           )
            row = cursor.fetchone()
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password)
        connection.set_client_encoding('utf-8')
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
        if self.use_db_timestamps:
            sql = """ CREATE TABLE {marker_table} (
                      update_id TEXT PRIMARY KEY,
                      target_table TEXT,
                      inserted TIMESTAMP DEFAULT NOW())
                  """.format(marker_table=self.marker_table)
        else:
            sql = """ CREATE TABLE {marker_table} (
                      update_id TEXT PRIMARY KEY,
                      target_table TEXT,
                      inserted TIMESTAMP);
                  """.format(marker_table=self.marker_table)

        try:
            cursor.execute(sql)
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                pass
            else:
                raise
        connection.close()

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")


class MultiReplacer(object):
    """
    Object for one-pass replace of multiple words
    Substituted parts will not be matched against other replace patterns, as opposed to when using multipass replace.
    The order of the items in the replace_pairs input will dictate replacement precedence.
    Constructor arguments:
    replace_pairs -- list of 2-tuples which hold strings to be replaced and replace string
    Usage:
    .. code-block:: python
        >>> replace_pairs = [("a", "b"), ("b", "c")]
        >>> MultiReplacer(replace_pairs)("abcd")
        'bccd'
        >>> replace_pairs = [("ab", "x"), ("a", "x")]
        >>> MultiReplacer(replace_pairs)("ab")
        'x'
        >>> replace_pairs.reverse()
        >>> MultiReplacer(replace_pairs)("ab")
        'xb'
    """

    def __init__(self, replace_pairs):
        """
        Initializes a MultiReplacer instance.
        :param replace_pairs: list of 2-tuples which hold strings to be replaced and replace string.
        :type replace_pairs: tuple
        """
        replace_list = list(replace_pairs)  # make a copy in case input is iterable
        self._replace_dict = dict(replace_list)
        pattern = '|'.join(re.escape(x) for x, y in replace_list)
        self._search_re = re.compile(pattern)

    def _replacer(self, match_object):
        # this method is used as the replace function in the re.sub below
        return self._replace_dict[match_object.group()]

    def __call__(self, search_string):
        # using function replacing for a per-result replace
        return self._search_re.sub(self._replacer, search_string)

