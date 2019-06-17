"""
    test_melder.py
"""
import datetime
import queue
import logging
import pytest
# pylint: disable=import-error
from frivenmeld.melder import Melder
from frivenmeld.combining_engine import CombiningEngine

from frivenmeld.loggingsetup import init_logging

init_logging(logging.DEBUG)

@pytest.fixture()
def metrics_collector():
    """
        Fixture of an instantiaed MetricsCollector object
    """

    class MockMetricsCollector():
        """
            Mocks MetricsCollector
        """

        def increment_matches(self):
            """
                mimicks real increment_matches()
            """
            pass

        def add_sample_row(self, row_dict):
            """
                mimicks real add_sample_row()
            """
            pass

    return MockMetricsCollector()


# pylint: disable=too-few-public-methods
@pytest.fixture()
def mysql_writer():
    """
        Fixture of an instantiaed MysqlWriter object
    """

    class MockMysqlWriter():
        """
            Mocks MysqlWriter
        """

        def add_record(self, match_record):
            """
                mimicks real add_record()
            """
            pass


    return MockMysqlWriter()


@pytest.fixture()
def mysql_loader():
    """
        Fixture of an instantiaed MysqlLoader object
    """

    class MockMysqlLoader():
        """
            Mocks MysqlLoader
        """

        def __init__(self):
            self._queue = queue.Queue()

        def _load_queue(self):
            """
                Adds simulated data to the queue
            """
            self._queue.put({'classification': 'controversial',
                             'firstname': 'Anthony',
                             'id': 765624,
                             'last_active_date': datetime.date(2016, 12, 29),
                             'lastname': 'Nistler',
                             'location': 'adamsville',
                             'specialty': 'Dermatology'})
            self._queue.put({'classification': 'contributor',
                             'firstname': 'Judy',
                             'id': 900062,
                             'last_active_date': datetime.date(2016, 12, 25),
                             'lastname': 'Nistler',
                             'location': 'attalla',
                             'specialty': 'Neurology'})
            self._queue.put({'classification': 'popular',
                             'firstname': 'Kyle',
                             'id': 916915,
                             'last_active_date': datetime.date(2016, 12, 30),
                             'lastname': 'Nistler',
                             'location': 'arab',
                             'specialty': 'Family Medicine'})

            self._queue.put({'classification': 'popular',
                             'firstname': 'Kyle',
                             'id': 916915,
                             'last_active_date': datetime.date(2016, 12, 30),
                             'lastname': 'Queen',
                             'location': 'arab',
                             'specialty': 'Family Medicine'})

            self._queue.put({'classification': 'popular',
                             'firstname': 'Kyle',
                             'id': 916915,
                             'last_active_date': datetime.date(2016, 12, 30),
                             'lastname': 'Yarrow',
                             'location': 'arab',
                             'specialty': 'Family Medicine'})

        def set_initial_lastname(self, lastname):
            """
                mocked version of set_initial_lastname()
            """
            pass

        def start(self):
            """
                mocked version of start() - puts stuff in the queue
            """
            self._load_queue()

        def stop(self):
            """
                mocked version of stop()
            """
            while not self._queue.empty():
                self._queue.get()

        def get_queue(self):
            """
                mocked version of get_queue()
                returns the queue
            """
            return self._queue

    return MockMysqlLoader()

@pytest.fixture()
def friven_loader():
    """
        Fixture of an instantiaed FrivenLoader object
    """

    class MockFrivenLoader():
        """
            Mocked version of FrivenLoader
        """

        def __init__(self):
            self._queue = queue.Queue()

        def _load_queue(self):
            self._queue.put({'firstname': 'Rona',
                             'id': 72515,
                             'friendly_vendor_page': 1,
                             'friendly_vendor_row': 12,
                             'last_active_date': '2017-01-10',
                             'lastname': 'Nistler',
                             'practice_location': 'birmingham',
                             'specialty': 'Cardiology',
                             'user_type_classification': 'Lurker'})

            self._queue.put({'firstname': 'Kyle',
                             'id': 93519,
                             'friendly_vendor_page': 1,
                             'friendly_vendor_row': 13,
                             'last_active_date': '2017-01-05',
                             'lastname': 'Nistler',
                             'practice_location': 'arab',
                             'specialty': 'Family Medicine',
                             'user_type_classification': 'Contributor'})

            self._queue.put({'firstname': 'Kyle',
                             'id': 93519,
                             'friendly_vendor_page': 1,
                             'friendly_vendor_row': 14,
                             'last_active_date': '2017-01-05',
                             'lastname': 'Opom',
                             'practice_location': 'arab',
                             'specialty': 'Family Medicine',
                             'user_type_classification': 'Contributor'})

            self._queue.put({'firstname': 'Kyle',
                             'id': 93519,
                             'friendly_vendor_page': 2,
                             'friendly_vendor_row': 1,
                             'last_active_date': '2017-01-05',
                             'lastname': 'Redlin',
                             'practice_location': 'arab',
                             'specialty': 'Family Medicine',
                             'user_type_classification': 'Contributor'})

            self._queue.put({'firstname': 'Kyle',
                             'id': 93519,
                             'friendly_vendor_page': 2,
                             'friendly_vendor_row': 2,
                             'last_active_date': '2017-01-05',
                             'lastname': 'Zoc',
                             'practice_location': 'arab',
                             'specialty': 'Family Medicine',
                             'user_type_classification': 'Contributor'})

        def start(self):
            """
                mimicks real start()
            """
            self._load_queue()

        def stop(self):
            """
                mimicks real stop()
            """
            while not self._queue.empty():
                self._queue.get()

        def get_queue(self):
            """
                mimicks real get_queue()
            """
            return self._queue

    return MockFrivenLoader()


# pylint: disable=redefined-outer-name
def test_normal_usage(friven_loader, mysql_loader, mysql_writer, metrics_collector):
    """
        Runs through happy path flow to touch most parts of the code
    """

    combining_engine = CombiningEngine(metrics_collector=metrics_collector,
                                       report_date='2019-02-02',
                                       mysql_writer=mysql_writer)

    melder = Melder(friven_loader=friven_loader,
                    mysql_loader=mysql_loader,
                    combining_engine=combining_engine,
                    friven_timeout=1,
                    mysql_timeout=1)
    melder.meld()
