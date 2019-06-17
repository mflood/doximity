"""
   validation_test.py

   runs validation tests against
   the output table
"""
import os
import logging
import pymysql
import pymysql.cursors
import pytest
from frivenmeld.loggingsetup import APP_LOGNAME
from frivenmeld.loggingsetup import init_logging

init_logging(logging.DEBUG)

class SqlRunner():
    """
        Run validation queries against target table
    """
    def __init__(self, host, port, database, username, password):

        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password

        self._fq_friendly_vendor_match = "friendly_vendor_match"
        self._logger = logging.getLogger(APP_LOGNAME)

    def get_table(self):
        """
            returns the table name for friendly_vendor_match
        """
        return self._fq_friendly_vendor_match

    def _get_connection(self):
        """
            Connects to mysql and returns the
            pymysql connection object
        """
        # Connect to the database
        connection = pymysql.connect(host=self._host,
                                     port=self._port,
                                     user=self._username,
                                     password=self._password,
                                     db=self._database,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        return connection

    def query_dictionary(self, sql, params):
        """
            Utility method
            to query the db
        """
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                results = cursor.fetchall()
                return results

        finally:
            connection.close()


@pytest.fixture()
def sql_runner():
    """
        Fixture of an instantiaed SqlRunning object
    """

    runner = SqlRunner(host=os.getenv("WRITE_MYSQL_HOST"),
                       port=int(os.getenv("WRITE_MYSQL_PORT")),
                       database=os.getenv("WRITE_MYSQL_SCHEMA"),
                       username=os.getenv("WRITE_MYSQL_USER"),
                       password=os.getenv("WRITE_MYSQL_PASS"))

    return runner


# pylint: disable=redefined-outer-name
def test_record_count(sql_runner):
    """
        returns the number of user records
        in the mysql table
    """
    sql = """select count(*) as the_count
               from {table}
               where report_date= '2017-02-02'
          """.format(table=sql_runner.get_table())

    results = sql_runner.query_dictionary(sql=sql, params=())
    count = int(results[0]['the_count'])
    assert count == 151677


def test_sample_2200(sql_runner):
    """
                    id: 2200
           practice_id: 50
             firstname: Fern
              lastname: Angelovich
        classification: contributor
             specialty: Neurology
platform_registered_on: mobile
      last_active_date: 2016-12-25
              location: bon air

    {
         'firstname': 'Fern',
         'id': 109982,
         'last_active_date': '2017-01-24',
         'lastname': 'Angelovich',
         'practice_location': 'bon_air',
         'specialty': 'Neurology',
         'user_type_classification': 'Contributor'
    }

    """

    sample = {
        "doximity_user_id": 2200,
        "friendly_vendor_user_id": 109982,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2016-12-25",
        "friendly_vendor_last_active_date": "2017-01-24",
        "_friendly_vendor_page": 4,
        "_friendly_vendor_row": 141
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_150(sql_runner):
    """
                    id: 150
           practice_id: 75
             firstname: Rebecca
              lastname: Barbeau
        classification: contributor
             specialty: Radiology
platform_registered_on: mobile
      last_active_date: 2016-12-28
                    id: 75
                  name: generic_clinic_74
              location: centreville

        'firstname': 'Rebecca',
        'id': 76983,
        'last_active_date': '2016-12-31',
        'lastname': 'Barbeau',
        'practice_location': 'centreville',
        'specialty': 'Radiology',
        'user_type_classification': 'Contributor'

    """
    sample = {
        "doximity_user_id": 150,
        "friendly_vendor_user_id": 76983,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2016-12-28",
        "friendly_vendor_last_active_date": "2016-12-31",
        "_friendly_vendor_page": 7,
        "_friendly_vendor_row": 416
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_5(sql_runner):
    """
                    id: 5
           practice_id: 76
             firstname: Paul
              lastname: Belanger
        classification: controversial
             specialty: Anesthesiology
platform_registered_on: website
      last_active_date: 2017-01-25
                    id: 76
                  name: generic_clinic_75
              location: chalkville

        'firstname': 'Paul',
        'id': 66148,
        'last_active_date': '2017-02-02',
        'lastname': 'Belanger',
        'practice_location': 'chalkville',
        'specialty': 'Anesthesiology',
        'user_type_classification': 'Leader'

    """
    sample = {
        "doximity_user_id": 5,
        "friendly_vendor_user_id": 66148,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-25",
        "friendly_vendor_last_active_date": "2017-02-02",
        "_friendly_vendor_page": 10,
        "_friendly_vendor_row": 47
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_325(sql_runner):
    """
                    id: 325
           practice_id: 22
             firstname: Johnnie
              lastname: Boettcher
        classification: contributor
             specialty: Cardiology
platform_registered_on: mobile
      last_active_date: 2017-01-22
                    id: 22
                  name: generic_clinic_21
              location: ashville

        'firstname': 'Johnnie',
        'id': 91841,
        'last_active_date': '2017-01-01',
        'lastname': 'Boettcher',
        'practice_location': 'ashville',
        'specialty': 'Cardiology',
        'user_type_classification': 'Contributor

    """
    sample = {
        "doximity_user_id": 325,
        "friendly_vendor_user_id": 91841,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-22",
        "friendly_vendor_last_active_date": "2017-01-01",
        "_friendly_vendor_page": 13,
        "_friendly_vendor_row": 433
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_20(sql_runner):
    """
                    id: 20
           practice_id: 46
             firstname: Sara
              lastname: Calloway
        classification: controversial
             specialty: Family Medicine
platform_registered_on: mobile
      last_active_date: 2017-01-03
                    id: 46
                  name: generic_clinic_45
              location: blue ridge

        'firstname': 'Sara',
        'id': 64629,
        'last_active_date': '2017-01-13',
        'lastname': 'Calloway',
        'practice_location': 'blue_ridge',
        'specialty': 'Family Medicine',
        'user_type_classification': 'Contributor'

    """
    sample = {
        "doximity_user_id": 20,
        "friendly_vendor_user_id": 64629,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-03",
        "friendly_vendor_last_active_date": "2017-01-13",
        "_friendly_vendor_page": 20,
        "_friendly_vendor_row": 824
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_495(sql_runner):
    """
                    id: 495
           practice_id: 82
             firstname: Juliann
              lastname: Calzada
        classification: contributor
             specialty: Cardiology
platform_registered_on: mobile
      last_active_date: 2016-12-26
              location: citronelle

        'firstname': 'Juliann',
        'id': 17225,
        'last_active_date': '2017-01-31',
        'lastname': 'Calzada',
        'practice_location': 'citronelle',
        'specialty': 'Cardiology',
        'user_type_classification': 'Contributor'

    """
    sample = {
        "doximity_user_id": 495,
        "friendly_vendor_user_id": 17225,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2016-12-26",
        "friendly_vendor_last_active_date": "2017-01-31",
        "_friendly_vendor_page": 20,
        "_friendly_vendor_row": 879
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_830(sql_runner):
    """
                    id: 830
           practice_id: 7
             firstname: Jodie
              lastname: Casey
        classification: news_reader
             specialty: Family Medicine
platform_registered_on: mobile
      last_active_date: 2017-01-19
                    id: 7
                  name: generic_clinic_6
              location: alexander city

        'firstname': 'Jodie',
        'id': 116274,
        'last_active_date': '2016-12-26',
        'lastname': 'Casey',
        'practice_location': 'alexander_city',
        'specialty': 'Family Medicine',
        'user_type_classification': 'Leader'

    """
    sample = {
        "doximity_user_id": 830,
        "friendly_vendor_user_id": 116274,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-19",
        "friendly_vendor_last_active_date": "2016-12-26",
        "_friendly_vendor_page": 23,
        "_friendly_vendor_row": 33
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_24590(sql_runner):
    """
                    id: 24590
           practice_id: 32
             firstname: Effie
              lastname: Eakins
        classification: contributor
             specialty: Orthopedics
platform_registered_on: website
      last_active_date: 2016-12-31
                    id: 32
                  name: generic_clinic_31
              location: bay minette

        'firstname': 'Effie',
        'id': 54779,
        'last_active_date': '2016-12-31',
        'lastname': 'Eakins',
        'practice_location': 'bay_minette',
        'specialty': 'Orthopedics',
        'user_type_classification': 'Contributor'

    """
    sample = {
        "doximity_user_id": 24590,
        "friendly_vendor_user_id": 54779,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2016-12-31",
        "friendly_vendor_last_active_date": "2016-12-31",
        "_friendly_vendor_page": 39,
        "_friendly_vendor_row": 672
    }
    pull_and_check_sample(sql_runner, sample)

def test_sample_85(sql_runner):
    """
                    id: 85
           practice_id: 31
             firstname: Olive
              lastname: Kallman
        classification: news_reader
             specialty: Family Medicine
platform_registered_on: website
      last_active_date: 2017-01-02
                    id: 31
                  name: generic_clinic_30
              location: banks

        'firstname': 'Olive',
        'id': 85473,
        'last_active_date': '2017-01-25',
        'lastname': 'Kallman',
        'practice_location': 'banks',
        'specialty': 'Family Medicine',
        'user_type_classification': 'Contributor'
    """
    sample = {
        "doximity_user_id": 85,
        "friendly_vendor_user_id": 85473,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-02",
        "friendly_vendor_last_active_date": "2017-01-25",
        "_friendly_vendor_page": 71,
        "_friendly_vendor_row": 399
    }
    pull_and_check_sample(sql_runner, sample)

def test_sample_105(sql_runner):
    """
                    id: 105
           practice_id: 63
             firstname: Misty
              lastname: Lean
        classification: controversial
             specialty: Dermatology
platform_registered_on: mobile
      last_active_date: 2016-12-25
                    id: 63
                  name: generic_clinic_62
              location: cahaba heights

        'firstname': 'Misty',
        'id': 53206,
        'last_active_date': '2017-01-08',
        'lastname': 'Lean',
        'practice_location': 'cahaba_heights',
        'specialty': 'Dermatology',
        'user_type_classification': 'Contributor'

    """
    sample = {
        "doximity_user_id": 105,
        "friendly_vendor_user_id": 53206,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2016-12-25",
        "friendly_vendor_last_active_date": "2017-01-08",
        "_friendly_vendor_page": 78,
        "_friendly_vendor_row": 676
    }
    pull_and_check_sample(sql_runner, sample)

def test_sample_75(sql_runner):
    """

                    id: 75
           practice_id: 15
             firstname: Ryan
              lastname: Mcaferty
        classification: news_reader
             specialty: Radiology
platform_registered_on: mobile
      last_active_date: 2017-01-11
                    id: 15
                  name: generic_clinic_14
              location: arab

        'firstname': 'Ryan',
        'id': 103455,
        'last_active_date': '2016-12-30',
        'lastname': 'Mcaferty',
        'practice_location': 'arab',
        'specialty': 'Radiology',
        'user_type_classification': 'Lurker'

    """

    sample = {
        "doximity_user_id": 75,
        "friendly_vendor_user_id": 103455,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-11",
        "friendly_vendor_last_active_date": "2016-12-30",
        "_friendly_vendor_page": 88,
        "_friendly_vendor_row": 330
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_130(sql_runner):
    """
                    id: 130
           practice_id: 14
             firstname: Adam
              lastname: Mustard
        classification: contributor
             specialty: Radiology
platform_registered_on: mobile
      last_active_date: 2017-01-27
                    id: 14
                  name: generic_clinic_13
              location: anniston

        'firstname': 'Adam',
        'id': 596,
        'last_active_date': '2017-01-08',
        'lastname': 'Mustard',
        'practice_location': 'anniston',
        'specialty': 'Radiology',
        'user_type_classification': 'Contributor'

    """

    sample = {
        "doximity_user_id": 130,
        "friendly_vendor_user_id": 596,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-27",
        "friendly_vendor_last_active_date": "2017-01-08",
        "_friendly_vendor_page": 98,
        "_friendly_vendor_row": 267
    }
    pull_and_check_sample(sql_runner, sample)


def test_sample_125(sql_runner):
    """
                    id: 125
           practice_id: 77
             firstname: Brandy
              lastname: Ridenhour
        classification: contributor
             specialty: Urology
platform_registered_on: website
      last_active_date: 2016-12-27
                    id: 77
                  name: generic_clinic_76
              location: chatom

         'firstname': 'Brandy',
         'id': 129519,
         'last_active_date': '2016-12-29',
         'lastname': 'Ridenhour',
         'practice_location': 'chatom',
         'specialty': 'Urology',
         'user_type_classification': 'Leader'

    """
    sample = {
        "doximity_user_id": 125,
        "friendly_vendor_user_id": 129519,
        "is_doximity_user_active": 0,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2016-12-27",
        "friendly_vendor_last_active_date": "2016-12-29",
        "_friendly_vendor_page": 115,
        "_friendly_vendor_row": 779
    }
    pull_and_check_sample(sql_runner, sample)

def test_sample_1545(sql_runner):
    """
                    id: 1545
           practice_id: 46
             firstname: Janet
              lastname: Shinn
        classification: contributor
             specialty: Urology
platform_registered_on: mobile
      last_active_date: 2017-01-27
                    id: 46
                  name: generic_clinic_45
              location: blue ridge

        'firstname': 'Janet',
        'id': 49985,
        'last_active_date': '2017-01-04',
        'lastname': 'Shinn',
        'practice_location': 'blue_ridge',
        'specialty': 'Urology',
        'user_type_classification': 'Contributor'

    """
    sample = {
        "doximity_user_id": 1545,
        "friendly_vendor_user_id": 49985,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-27",
        "friendly_vendor_last_active_date": "2017-01-04",
        "_friendly_vendor_page": 126,
        "_friendly_vendor_row": 219
    }
    pull_and_check_sample(sql_runner, sample)

def test_sample_2420(sql_runner):
    """
                    id: 2420
           practice_id: 63
             firstname: Nydia
              lastname: Stern
        classification: contributor
             specialty: Radiology
platform_registered_on: mobile
      last_active_date: 2017-01-18
                    id: 63
                  name: generic_clinic_62
              location: cahaba heights

        'firstname': 'Nydia',
        'id': 52428,
        'last_active_date': '2016-12-27',
        'lastname': 'Stern',
        'practice_location': 'cahaba_heights',
        'specialty': 'Radiology',
        'user_type_classification': 'Contributor'

    """
    sample = {

        "doximity_user_id": 2420,
        "friendly_vendor_user_id": 52428,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 0,
        "classification_match": 1,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-18",
        "friendly_vendor_last_active_date": "2016-12-27",
        "_friendly_vendor_page": 132,
        "_friendly_vendor_row": 48
    }
    pull_and_check_sample(sql_runner, sample)

def test_sample_250040(sql_runner):
    """

    This is the last record from Friendly Vendor

                    id: 250040
           practice_id: 15
             firstname: Ronald
              lastname: Zywiec
        classification: news_reader
             specialty: Urology
platform_registered_on: mobile
      last_active_date: 2017-01-10
                    id: 15
                  name: generic_clinic_14
              location: arab

        'firstname': 'Ronald',
        'id': 118231,
        'last_active_date': '2017-01-17',
        'lastname': 'Zywiec',
        'practice_location': 'arab',
        'specialty': 'Urology',
        'user_type_classification': 'Lurker'
    """
    sample = {

        "doximity_user_id": 250040,
        "friendly_vendor_user_id": 118231,
        "is_doximity_user_active": 1,
        "is_friendly_vendor_user_active": 1,
        "classification_match": 0,
        "location_match": 1,
        "specialty_match": 1,
        "report_date": "2017-02-02",
        "doximity_last_active_date": "2017-01-10",
        "friendly_vendor_last_active_date": "2017-01-17",
        "_friendly_vendor_page": 152,
        "_friendly_vendor_row": 677
    }
    pull_and_check_sample(sql_runner, sample)

def pull_and_check_sample(sql_runner, sample):
    """
        Queries the target table for the
        sample and compares the values.

        Sample contains the data we expect to find
        for the doximity_user_id in the target table

        exxample sample:

        {
            "doximity_user_id": 2420,
            "friendly_vendor_user_id": 52428,
            "is_doximity_user_active": 1,
            "is_friendly_vendor_user_active": 0,
            "classification_match": 1,
            "location_match": 0,
            "specialty_match": 1,
            "report_date": "2017-02-02",
            "doximity_last_active_date": "2017-01-18",
            "friendly_vendor_last_active_date": "2016-12-27",
            "_friendly_vendor_page": 132,
            "_friendly_vendor_row": 481
        }
    """
    sql = """select
                  doximity_user_id
                , friendly_vendor_user_id
                , is_doximity_user_active
                , is_friendly_vendor_user_active
                , classification_match
                , location_match
                , specialty_match
                , date_format(report_date, '%%Y-%%m-%%d') as report_date
                , date_format(doximity_last_active_date, '%%Y-%%m-%%d') as doximity_last_active_date
                , date_format(friendly_vendor_last_active_date, '%%Y-%%m-%%d') as friendly_vendor_last_active_date
                , _friendly_vendor_page
                , _friendly_vendor_row
           from {table}
               where doximity_user_id = %s
               and report_date = '2017-02-02'
          """.format(table=sql_runner.get_table())

    results = sql_runner.query_dictionary(sql=sql, params=(sample["doximity_user_id"]))
    for key, value in sample.items():
        assert results[0][key] == value

# end
