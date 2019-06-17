"""
   mysql_loader.py

   Loads data from MySQL users / user_practice
   table
"""
import os
import logging
import queue
import threading
# pylint: disable=import-error
import pymysql
import pymysql.cursors
from frivenmeld.loggingsetup import APP_LOGNAME

class MysqlLoaderException(Exception):
    """
        Exception to raise when things go South
    """
    pass

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
class MysqlLoader(threading.Thread):
    """
        Maintains a Queue of FriendlyVender user objects
        Ordered by Last Name
    """
    def __init__(self, host, port, database, username, password):

        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password

        self._user_queue = None
        self._thread_plunger = None
        self._batch_size = None
        self._fq_user_table = "user"
        self._fq_user_practice_table = "user_practice"

        # by default we start with empty string
        # so we start alphabetically with the first
        # lastname in the mysql user table.
        # if we set this to some other value,
        # say "flood", then mysql will skip
        # all the lastnames that come before "flood"
        self._initial_lastname = ""
        self._logger = logging.getLogger(APP_LOGNAME)

        super(MysqlLoader, self).__init__()

    def set_initial_lastname(self, lastname):
        """
            override the default initial
            last_name so that the loader
            doesn't process any more
            records than necessary
            if we know we don't need
            to consider lastnames before
            this one.  Applicable when we are
            running multiple instances of the app
            each responsible for their own range
        """
        self._initial_lastname = lastname
        self._logger.info("MysqlLoader configured to start with lastname '%s'",
                          self._initial_lastname)

    def get_connection(self):
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
            raise MysqlLoaderException(error)

    def _get_user_count(self):
        """
            returns the number of user records
            in the mysql table
        """
        sql = """select count(*) as the_count
                   from {user_table}""".format(user_table=self._fq_user_table)

        results = self._query_dictionary(sql=sql)
        count = int(results[0]['the_count'])
        return count

    def _get_percentage_count(self, percent):
        """
            returns the number of records
            that would make PERCENT
            of the total number of user
            records in the table
        """
        assert 1 <= percent <= 100

        user_count = self._get_user_count()
        percent_as_float = percent / 100.0
        percentage_count = int(percent_as_float * user_count)
        self._logger.info("%s percent of %s is %s",
                          percent,
                          user_count,
                          percentage_count)
        return percentage_count

    def init_queue_data_percent(self, percent):
        """
            sets the queuesize and
            db_batch size to values
            that would allow PERCENT
            number of records in memory
            at a time
        """
        percentage_count = self._get_percentage_count(percent=percent)
        queue_max_size = int(percentage_count * 0.2)
        self.init_queue(queue_maxsize=queue_max_size,
                        db_batch_size=percentage_count-queue_max_size)

    def init_queue(self, queue_maxsize, db_batch_size):
        """
            Instantiates the queue,
            setting the maxsize

            also sets the db_batchsize
        """
        assert queue_maxsize > 0
        assert db_batch_size > 0

        self._user_queue = queue.Queue(maxsize=queue_maxsize)
        self._batch_size = db_batch_size
        self._logger.info("MysqlLoader configured with queue size %s and DB batch size %s",
                          queue_maxsize,
                          self._batch_size)

    def get_queue(self):
        """
            Accessor for the queue
        """
        return self._user_queue

    def _query_dictionary(self, sql, *args):
        """
            Utility method
            to query the db
        """
        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, list(args))
                results = cursor.fetchall()
                return results

        except pymysql.err.InternalError as error:
            self._logger.error("SELECT ERROR: %s", error)
            self._logger.error(sql)
            raise MysqlLoaderException(error)
        finally:
            connection.close()

    def stop(self):
        """
            Empties the _thread_plunger queue
            and drains all items from the user_queue
            so the run() method can exit gracefully
        """
        # we should only be calling this once
        assert not self._thread_plunger.empty()

        if self._thread_plunger:
            self._logger.info("Pulling the plug on MysqlLoader")
            self._thread_plunger.get(timeout=1)
            while True:
                try:
                    self._user_queue.get(timeout=1)
                except queue.Empty:
                    self._logger.info("MysqlLoader queue drained")
                    return


    def run(self):
        """
            This is the method that
            the thread's start()
            method invokes
        """

        assert self._user_queue
        assert self._batch_size

        # Having a single item in this queue
        # is what keeps the run loop going
        self._thread_plunger = queue.Queue()
        self._thread_plunger.put("plug")

        batchsize = self._batch_size

        sql = """
            select user.id
                 , user.firstname
                 , user.lastname
                 , user.classification
                 , user.specialty
                 , user_practice.location
                 , last_active_date
              from {user_table} as user
             inner join {user_practice_table} as user_practice
                on user.practice_id=user_practice.id
             where user.lastname > %s
               or  (user.lastname = %s and user.id > %s)
             order by user.lastname, user.id
             limit {batchsize};
        """.format(user_table=self._fq_user_table,
                   user_practice_table=self._fq_user_practice_table,
                   batchsize=batchsize)

        current_lastname = self._initial_lastname
        current_id = 0
        while True:

            if self._thread_plunger.empty():
                self._logger.info("Someone pulled the plug on MysqlLoader. Bailing...")
                return

            self._logger.info("querying mysql '%s', %s, %s",
                              current_lastname,
                              current_id,
                              batchsize)

            next_id = current_id
            results = self._query_dictionary(sql,
                                             current_lastname,
                                             current_lastname,
                                             current_id)
            if results:
                self._logger.info("MysqlLoader selected %s records from %s to %s",
                                  len(results), results[0]['lastname'],
                                  results[-1]['lastname'])
            else:
                self._logger.info("MysqlLoader select came up empty.")

            for row in results:
                #sys.stdout.write('+')
                #sys.stdout.flush()
                current_lastname = row['lastname']
                next_id = row['id']
                self._user_queue.put(row)

            if next_id == current_id:
                # the id did not change
                # we're done!
                break

            current_id = next_id


if __name__ == "__main__":
    #
    # Demonstrates basic usage of this class

    from frivenmeld.loggingsetup import init_logging
    init_logging(logging.DEBUG)

    # pylint: disable=invalid-name
    try:
        mysql_loader = MysqlLoader(host=os.getenv("MYSQL_HOST"),
                                   port=int(os.getenv("MYSQL_PORT")),
                                   database=os.getenv("MYSQL_SCHEMA"),
                                   username=os.getenv("MYSQL_USER"),
                                   password=os.getenv("MYSQL_PASS"))

        mysql_loader.init_queue_data_percent(1)
    except MysqlLoaderException as error:
        logging.getLogger(APP_LOGNAME).error(error)

    mysql_loader.start()

    num_processed = 0
    while True:
        user = mysql_loader.get_queue().get(timeout=1)
        if num_processed >= 20000:
            mysql_loader.stop()
            break
        num_processed += 1

    print("Main thread done")
#
