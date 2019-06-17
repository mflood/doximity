"""
   mysql_writer.py

   Writes records to friendly_vendor_match table
"""
import os
import logging
import queue
# pylint: disable=import-error
import pymysql
import pymysql.cursors
from frivenmeld.loggingsetup import APP_LOGNAME

class MysqlWriterException(Exception):
    """
        Exception to raise when things go South
    """
    pass

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
class MysqlWriter():
    """
        Writes records to friendly_vendor_match table
    """
    def __init__(self,
                 host,
                 port,
                 database,
                 username,
                 password):

        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password

        self._write_queue = None
        self._worker_id = 0
        self._fq_friendly_vendor_match = "friendly_vendor_match"

        self._dry_run = False

        self._logger = logging.getLogger(APP_LOGNAME)

        self._fields = [
            "doximity_user_id",
            "friendly_vendor_user_id",
            "is_doximity_user_active",
            "is_friendly_vendor_user_active",
            "classification_match",
            "location_match",
            "specialty_match",
            "report_date",
            "doximity_last_active_date",
            "friendly_vendor_last_active_date",
            "_worker_id",
            "_friendly_vendor_page",
            "_friendly_vendor_row",
        ]


    def set_worker_id(self, worker_id):
        """
            set the worker ID associated
            with records created

            use worker_id = 0 (default)
            if you are not using multiple workers
        """

        self._worker_id = int(worker_id)

        self._logger.info("MysqlWriter _worker_id set to %s", self._worker_id)

    def set_friendly_vendor_match_table(self, tablename):
        """
            replace the default table name
        """
        self._fq_friendly_vendor_match = tablename

    def set_dry_run(self, is_dry_run):
        """
            Turn on/off _dry_run
        """
        self._dry_run = is_dry_run

    def _get_connection(self):
        """
            Connects to mysql and returns the
            pymysql connection object
        """
        # Connect to the database
        try:
            connection = pymysql.connect(host=self._host,
                                         port=self._port,
                                         user=self._username,
                                         password=self._password,
                                         db=self._database,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)

            return connection

        except pymysql.err.OperationalError as error:
            self._logger.error(error)
            raise MysqlWriterException(error)

    def init_queue(self, batchsize):
        """
            Instantiates the queue,
            setting the maxsize to batchsize
        """
        assert batchsize > 0

        self._write_queue = queue.Queue(maxsize=batchsize)
        self._logger.info("MysqlWriter configured with queue size %s",
                          batchsize)

    def remove_records_for_date(self, date_string):
        """
            Remove the records for the date
            so we can re-run the etl

            If we have a worker_id set, we
            only remove records that have our worker_id

            If we don't have a worker_id
            we delete all workers' data
        """
        sql = """
              delete from {friendly_vendor_match}
               where report_date = %s
        """.format(friendly_vendor_match=self._fq_friendly_vendor_match)

        params = [date_string]

        # if worker ID is greater than 0
        # we only delete our own records
        if self._worker_id:
            sql += " and _worker_id = %s"
            params.append(self._worker_id)
        self._execute(sql=sql, param_list=params)

    def add_record(self, match_record):
        """
            Add a record to the write queue
            if the queue is full (batchsize reached)
            then run inserts
        """

        try:
            self._write_queue.put(match_record, block=False)
        except queue.Full:
            self._logger.debug("MysqlWriter Queue is full. Running inserts.")
            self.run_inserts()
            # don't leave this guy out!
            self._write_queue.put(match_record, block=False)

    def run_inserts(self):
        """
            Reads the queue completely
            and creates a multi-insert sql statment
        """

        self._logger.info("MysqlWriter running inserts for all data in the queue.")
        # doximity_user_id, friendly_vendor_user_id, is_doximity_user_active, etc...
        field_list = ", ".join(self._fields)

        # ['%s', '%s', '%s']
        interpolation_list = ["%s" for _ in range(len(self._fields))]

        # ( %s, %s, %s )
        interpolation_string = "(%s)" % ", ".join(interpolation_list)

        sql = "INSERT INTO %s (%s) values \n" % (self._fq_friendly_vendor_match, field_list)

        value_list = []
        param_list = []
        # build an insert statement
        # with multiple (e.g. 100) (value1, value2) insert things
        while True:
            try:
                match_record = self._write_queue.get(block=False)

                # add worker_id
                match_record["_worker_id"] = self._worker_id

                # (%s, %s, %s)
                value_list.append(interpolation_string)

                try:
                    # [ 3, 'john', 23 ]
                    arg_list = [match_record[fieldname] for fieldname in self._fields]
                    # ... '3', 'john', 23
                    param_list.extend(arg_list)
                except KeyError:
                    self._logger.error("Configured Fields: %s", field_list)
                    self._logger.error("Row being processed: %s", match_record)
                    raise

            except queue.Empty:
                self._logger.debug("MysqlLoader queue is empty.")
                break

        if not value_list:
            # nothing in queue?
            return

        # (%s, %s),
        # (%s, %s),
        # (%s %s)
        sql += ",\n".join(value_list)

        self._execute(sql, param_list)

    def _execute(self, sql, param_list=None):
        """
            Utility method
            to execute DML
        """
        if not param_list:
            param_list = []

        if self._dry_run:
            self._logger.warning("DRY RUN - Skipping actual inserts")
            return

        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, param_list)
                cursor.execute("commit")

        except pymysql.err.InternalError as error:
            self._logger.error("SQL ERROR: %s", error)
            self._logger.error("Truncated SQL (2000 chars): %s", sql[:2000])
            raise MysqlWriterException(error)
        finally:
            connection.close()


if __name__ == "__main__":
    # Demo section
    # Shows the typical usage of this class
    # You'll need to source local_env.sh
    # before running this module as __main__

    from frivenmeld.loggingsetup import init_logging
    init_logging(logging.DEBUG)

    # pylint: disable=invalid-name
    try:
        test_mysql_writer = MysqlWriter(host=os.getenv("WRITE_MYSQL_HOST"),
                                        port=int(os.getenv("WRITE_MYSQL_PORT")),
                                        database=os.getenv("WRITE_MYSQL_SCHEMA"),
                                        username=os.getenv("WRITE_MYSQL_USER"),
                                        password=os.getenv("WRITE_MYSQL_PASS"))

        test_mysql_writer.init_queue(batchsize=10000)
    except MysqlWriterException as error:
        logging.getLogger(APP_LOGNAME).error(error)

    test_mysql_writer.remove_records_for_date('2017-02-02')

    count = 0
    for x in range(1005):
        count += 1
        dox_id = x
        friven_id = x

        test_match_record = {
            'report_date': '2017-05-02',
            'doximity_user_id': dox_id,
            'friendly_vendor_user_id': friven_id,
            'location_match': 1,
            'specialty_match': 0,
            'classification_match': 1,
            'doximity_last_active_date': '2017-01-02',
            'friendly_vendor_last_active_date': '2017-04-04',
            'is_doximity_user_active': 1,
            'is_friendly_vendor_user_active': 0,
            '_friendly_vendor_page': 18,
            '_friendly_vendor_row': 204,
        }

        test_mysql_writer.add_record(match_record=test_match_record)

    test_mysql_writer.run_inserts()
    print(count)

# end
