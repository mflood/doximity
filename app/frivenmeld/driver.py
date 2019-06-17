"""
	driver.py

	Main application
	Builds anbd coordinates the melding process

"""
import logging
import datetime
import argparse
import os
from frivenmeld.loggingsetup import APP_LOGNAME
from frivenmeld.loggingsetup import init_logging
from frivenmeld.combining_engine import CombiningEngine
from frivenmeld.metrics_collector import MetricsCollector
from frivenmeld.melder import Melder
from frivenmeld.friendly_vendor.friven_loader import FrivenLoader
from frivenmeld.doximity.mysql_loader import MysqlLoader
from frivenmeld.doximity.mysql_writer import MysqlWriter

DOXIMITY_WORKING_DATA_PERCENT = 10
FRIENDLY_WORKING_DATA_PERCENT = 10

def valid_date(date_as_string):
    """
        Used to validate the report_date
        argument parsed by the argparser
    """
    try:
        # don't convert date_as_string, just make sure
        # it's in the right format
        datetime.datetime.strptime(date_as_string, "%Y-%m-%d")
        return date_as_string
    except ValueError:
        msg = "Date is not in YYYY-MM-DD format: '{0}'.".format(date_as_string)
        raise argparse.ArgumentTypeError(msg)

def parse_args(argv=None):
    """
        Parse command line args
    """
    parser = argparse.ArgumentParser(description="Main Driver for Frivenmeld")

    parser.add_argument('-v',
                        action="store_true",
                        dest="verbose",
                        required=False,
                        help="Debug output")

    parser.add_argument('--dryrun',
                        action="store_true",
                        dest="dry_run",
                        required=False,
                        help="Don't connect or write to Target MySQL server")

    parser.add_argument("--delete-existing",
                        dest="delete_existing",
                        default=False,
                        action="store_true",
                        help="Delete all records for the report_date for "
                             "this worker from the target table before loading")

    parser.add_argument("--report-date",
                        dest="report_date",
                        default='2017-02-02',
                        type=valid_date,
                        help="(YYYY-MM-DD) The date to store as "
                             "report_date and to compare last active dates to.")

    parser.add_argument("--workerid",
                        dest="worker_id",
                        default=0,
                        required=False,
                        type=(int),
                        help="Worker ID to associate with records written to the Table")

    parser.add_argument("--startpage",
                        dest="start_page",
                        default=1,
                        type=int,
                        required=False,
                        help="Start with this page of the Friendly Vendor Users api")

    parser.add_argument('--endpage',
                        dest="end_page",
                        default=None,
                        type=int,
                        required=False,
                        help="Stop after reaching this page of the Friendly Vendor Users api")

    parser.add_argument("--timeout",
                        dest="timeout",
                        default=20,
                        type=int,
                        required=False,
                        help="Set the timeout for reading from queues. "
                             "You can decrease this to around 2 seconds on fast networks")

    parser.add_argument('--output-batchsize',
                        dest="output_batchsize",
                        default=10000,
                        required=False,
                        type=int,
                        help="Batch inserts into this many statements")

    results = parser.parse_args(argv)
    return results

def load_config():
    """
        Loads environment variables
        into a dictionary and raises
        and exception if one is missing
    """

    config = {}
    env_vars = [
        "FRIENDLY_VENDOR_API_URL",
        "MYSQL_HOST",
        "MYSQL_PORT",
        "MYSQL_SCHEMA",
        "MYSQL_USER",
        "MYSQL_PASS",
        "MYSQL_FQ_USER_TABLE",
        "MYSQL_FQ_USER_PRATICE_TABLE",
        "WRITE_MYSQL_HOST",
        "WRITE_MYSQL_PORT",
        "WRITE_MYSQL_SCHEMA",
        "WRITE_MYSQL_USER",
        "WRITE_MYSQL_PASS",
        "WRITE_MYSQL_FQ_MATCH_TABLE",
    ]
    for item in env_vars:
        config[item] = os.getenv(item)
        if not config[item]:
            raise Exception("Environment variable {} is not configured".format(item))

    config["MYSQL_PORT"] = int(config["MYSQL_PORT"])
    config["WRITE_MYSQL_PORT"] = int(config["WRITE_MYSQL_PORT"])
    return config

def print_ddl():
    """
        Print out the DDL used to create the target table
    """

    create_ddl = """ create table friendly_vendor_match (
    friendly_vendor_match_id           bigint unsigned not null auto_increment primary key,
    doximity_user_id                      int unsigned not null,
    friendly_vendor_user_id               int unsigned not null,
    is_doximity_user_active           tinyint unsigned not null,
    is_friendly_vendor_user_active    tinyint unsigned not null,
    classification_match              tinyint unsigned not null,
    location_match                    tinyint unsigned not null,
    specialty_match                   tinyint unsigned not null,
    report_date                          date not null,
    doximity_last_active_date            date not null,
    friendly_vendor_last_active_date     date not null,
    _worker_id                        tinyint unsigned not null,
    _friendly_vendor_page                  int unsigned not null,
    _friendly_vendor_row                   int unsigned not null,
    _create_time                     timestamp not null default current_timestamp,
    index(report_date, _worker_id),
    unique(report_date, doximity_user_id, friendly_vendor_user_id)
);

    """
    print("SQL DDL: {}".format(create_ddl))


def main():
    """
        Entry point to program
    """
    arg_object = parse_args()

    if arg_object.verbose:
        init_logging(loglevel=logging.DEBUG)
    else:
        init_logging(loglevel=logging.INFO)

    mcollector = MetricsCollector()

    config = load_config()

    # Configure the FrivenLoader
    #
    api_url = config["FRIENDLY_VENDOR_API_URL"]
    friven_loader = FrivenLoader(friven_api_url=api_url)

    friven_loader.init_queue_data_percent(percent=FRIENDLY_WORKING_DATA_PERCENT)

    friven_loader.set_page_range(first_page_number=arg_object.start_page,
                                 last_page_number=arg_object.end_page)

    # Configure the MysqlLoader
    #
    mysql_loader = MysqlLoader(host=config["MYSQL_HOST"],
                               port=config["MYSQL_PORT"],
                               database=config["MYSQL_SCHEMA"],
                               username=config["MYSQL_USER"],
                               password=config["MYSQL_PASS"])
    mysql_loader.init_queue_data_percent(percent=DOXIMITY_WORKING_DATA_PERCENT)

    # Configure MysqlWriter
    #
    mysql_writer = MysqlWriter(host=config["WRITE_MYSQL_HOST"],
                               port=config["WRITE_MYSQL_PORT"],
                               database=config["WRITE_MYSQL_SCHEMA"],
                               username=config["WRITE_MYSQL_USER"],
                               password=config["WRITE_MYSQL_PASS"])

    mysql_writer.set_friendly_vendor_match_table(tablename=config["WRITE_MYSQL_FQ_MATCH_TABLE"])
    mysql_writer.init_queue(batchsize=arg_object.output_batchsize)
    mysql_writer.set_worker_id(worker_id=arg_object.worker_id)
    if arg_object.delete_existing:
        mysql_writer.remove_records_for_date(arg_object.report_date)

    if arg_object.dry_run:
        mysql_writer.set_dry_run(is_dry_run=arg_object.dry_run)

    combining_engine = CombiningEngine(metrics_collector=mcollector,
                                       report_date=arg_object.report_date,
                                       mysql_writer=mysql_writer)

    # Configure the Melder
    #
    melder = Melder(friven_loader=friven_loader,
                    mysql_loader=mysql_loader,
                    combining_engine=combining_engine,
                    friven_timeout=arg_object.timeout,
                    mysql_timeout=arg_object.timeout)
    # Do the work
    melder.meld()
    mysql_writer.run_inserts()

    # Gather results
    mcollector.mark_end_time()
    mcollector.print_summary()

    # Print the DDL
    print_ddl()

    logging.getLogger(APP_LOGNAME).info("Job complete.")


if __name__ == "__main__":
    main()

#end
