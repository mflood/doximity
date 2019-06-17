
import datetime
import pytest
from frivenmeld.combining_engine import CombiningEngine
from frivenmeld.metrics_collector import MetricsCollector
from frivenmeld.doximity.mysql_writer import MysqlWriter



@pytest.fixture()
def mysql_writer():

    writer = MysqlWriter(host=None,
                         port=None,
                         database=None,
                         username=None,
                         password=None)

    writer.init_queue(batchsize=1)
    writer.set_dry_run(is_dry_run=True)
    return writer

@pytest.fixture()
def metrics_collector():
    
    collector = MetricsCollector()
    return collector

@pytest.fixture()
def friven_list():

    friven_user_list = [ 
        {
            'firstname': 'Rona',
            'id': 72515,
            'last_active_date': '2017-01-10',
            'lastname': 'Nistler',
            'practice_location': 'birmingham',
            'specialty': 'Cardiology',
            'user_type_classification': 'Lurker',
            'friendly_vendor_page': 23,
            'friendly_vendor_row': 410
        }, {
            'firstname': 'Kyle',
            'id': 93519,
            'last_active_date': '2017-01-05',
            'lastname': 'Nistler',
            'practice_location': 'arab',
            'specialty': 'Family Medicine',
            'user_type_classification': 'Contributor',
            'friendly_vendor_page': 23,
            'friendly_vendor_row': 411
        }, {
            'firstname': 'Kyle',
            'id': 1111,
            'last_active_date': '2017-01-05',
            'lastname': 'Nistler',
            'practice_location': 'boston',
            'specialty': 'Family Medicine',
            'user_type_classification': 'Contributor',
            'friendly_vendor_page': 23,
            'friendly_vendor_row': 412
        }
    ]
    return friven_user_list

@pytest.fixture()
def mysql_list():

    mysql_user_list = [
        {
            'classification': 'controversial',
            'firstname': 'Anthony',
            'id': 765624,
            'last_active_date': datetime.date(2016, 12, 29),
            'lastname': 'Nistler',
            'location': 'adamsville',
            'specialty': 'Dermatology'
        }, {
            'classification': 'contributor',
            'firstname': 'Judy',
            'id': 900062,
            'last_active_date': datetime.date(2016, 12, 25),
            'lastname': 'Nistler',
            'location': 'attalla',
            'specialty': 'Neurology'
        }, {
            'classification': 'popular',
            'firstname': 'Kyle',
            'id': 916915,
            'last_active_date': datetime.date(2016, 12, 30),
            'lastname': 'Nistler',
            'location': 'arab',
            'specialty': 'Family Medicine'
        }
    ]
    return mysql_user_list



def test_combine(metrics_collector, mysql_writer, friven_list, mysql_list):

    combiner = CombiningEngine(metrics_collector=metrics_collector,
                               report_date = "2017-02-02",
                               mysql_writer=mysql_writer)

    combiner.combine(friven_user_list=friven_list,
                     mysql_user_list=mysql_list)

 
