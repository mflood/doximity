"""
    combining_engine.py
"""
import logging
import datetime
from frivenmeld.loggingsetup import APP_LOGNAME

class CombiningEngine():
    """
        Knows how to combine users
        and where to send the matched pairs

        Gets fed data by the Melder, which calls combine()
    """

    def __init__(self, metrics_collector, report_date, mysql_writer):
        """
            report_date: 2019-02-01
        """
        self._logger = logging.getLogger(APP_LOGNAME)
        self._mysql_writer = mysql_writer
        self._metrics_collector = metrics_collector
        self._report_date = datetime.datetime.strptime(report_date, "%Y-%m-%d").date()

    def combine(self, friven_user_list, mysql_user_list):
        """
            Takes a list of friven_users and mysql_users
            and matches them up

            Sends matches to the writer

            Example friven_user_list:

            [{'firstname': 'Rona',
              'id': 72515,
              'last_active_date': '2017-01-10',
              'lastname': 'Nistler',
              'practice_location': 'birmingham',
              'specialty': 'Cardiology',
              'user_type_classification': 'Lurker',
              'friendly_vendor_page': 23,
              'friendly_vendor_row': 410},
             {'firstname': 'Kyle',
              'id': 93519,
              'last_active_date': '2017-01-05',
              'lastname': 'Nistler',
              'practice_location': 'arab',
              'specialty': 'Family Medicine',
              'user_type_classification': 'Contributor',
              'friendly_vendor_page': 23,
              'friendly_vendor_page': 411}]

            Example mysql user list:

            [{'classification': 'controversial',
              'firstname': 'Anthony',
              'id': 765624,
              'last_active_date': datetime.date(2016, 12, 29),
              'lastname': 'Nistler',
              'location': 'adamsville',
              'specialty': 'Dermatology'},
             {'classification': 'contributor',
              'firstname': 'Judy',
              'id': 900062,
              'last_active_date': datetime.date(2016, 12, 25),
              'lastname': 'Nistler',
              'location': 'attalla',
              'specialty': 'Neurology'},
             {'classification': 'popular',
              'firstname': 'Kyle',
              'id': 916915,
              'last_active_date': datetime.date(2016, 12, 30),
              'lastname': 'Nistler',
              'location': 'arab',
              'specialty': 'Family Medicine'}]
        """

        self._logger.debug("Combining %s friven with %s mysql for '%s'",
                           len(friven_user_list),
                           len(mysql_user_list),
                           friven_user_list[0]['lastname'])


        mysql_dict = self._build_mysql_dict(user_list=mysql_user_list)

        for friven_user in friven_user_list:
            friven_user_key = self._make_match_key(firstname=friven_user['firstname'],
                                                   lastname=friven_user['lastname'])
            if friven_user_key in mysql_dict:
                self._process_match(mysql_user=mysql_dict[friven_user_key]['mysql_user'],
                                    friven_user=friven_user)

                # add friven user to friven_matched_users array
                # so we can track multi matches if they exist
                # so far.... they do not
                mysql_dict[friven_user_key]['friven_matched_users'].append(friven_user)
                if len(mysql_dict[friven_user_key]['friven_matched_users']) > 1:
                    self._logger.info("Found multi-match: %s", mysql_dict[friven_user_key])


    def _process_match(self, mysql_user, friven_user):
        """
            We have a single mysql (Doximity) user paired
            with a Friven (Friendly Vendor) user.

            Create the record that has all the info for the db insert
            and send the record to the mysql writer
        """
        match_record = self._create_match_record(mysql_user=mysql_user,
                                                 friven_user=friven_user)
        self._metrics_collector.add_sample_row(row_dict=match_record)
        self._metrics_collector.increment_matches()
        self._mysql_writer.add_record(match_record=match_record)


    def _create_match_record(self, mysql_user, friven_user):
        """
            Assemble the data that will end up as a record in the target table
        """

        mysql_last_active_date = mysql_user['last_active_date']
        friven_last_active_date = datetime.datetime.strptime(friven_user['last_active_date'],
                                                             "%Y-%m-%d").date()

        # active if within 30 days
        is_mysql_user_active = self._user_is_active(last_active_date=mysql_last_active_date)
        is_friven_user_active = self._user_is_active(last_active_date=friven_last_active_date)

        match_record = {
            'report_date': str(self._report_date),
            'doximity_user_id': mysql_user['id'],
            'friendly_vendor_user_id': friven_user['id'],
            'location_match': self._strings_are_equal(mysql_user['location'],
                                                      friven_user['practice_location']),
            'specialty_match': self._strings_are_equal(mysql_user['specialty'],
                                                       friven_user['specialty']),
            'classification_match': self._strings_are_equal(mysql_user['classification'],
                                                            friven_user['user_type_classification']),
            'doximity_last_active_date': str(mysql_last_active_date),
            'friendly_vendor_last_active_date': str(friven_last_active_date),
            'is_doximity_user_active': int(is_mysql_user_active),
            'is_friendly_vendor_user_active': int(is_friven_user_active),
            '_friendly_vendor_page': friven_user['friendly_vendor_page'],
            '_friendly_vendor_row': friven_user['friendly_vendor_row'],
        }
        return match_record


    def _user_is_active(self, last_active_date):
        """
            A user is active if the last_active_date is within 30 days
            of the report date
        """
        delta = self._report_date - last_active_date
        return delta.days <= 30

    def _strings_are_equal(self, value1, value2):
        """
            Magic juiciness to see if values are the same.
            lower case / underscores / strippin'
        """
        if value1.lower().strip().replace('_', ' ') == value2.lower().strip().replace('_', ' '):
            return 1
        return 0

    def _build_mysql_dict(self, user_list):
        """
            We're using first and last name
            for the key simply because we know that
            there are no duplicate first/last names
            in our system.  If there were duplicates,
            we need to be aware that only one of the users
            would end up in the dictionary.  In that case
            we would want to use a more complex strategy
            that might incorporate location or specialty.
        """
        return_dict = {}
        for user in user_list:
            user_key = self._make_match_key(firstname=user['firstname'],
                                            lastname=user['lastname'])
            assert user_key not in return_dict
            return_dict[user_key] = {'mysql_user': user,
                                     'friven_matched_users': []
                                    }
        return return_dict

    def _make_match_key(self, firstname, lastname):
        """
            returns some unique value based on
            firstname and lastname
            something we can use as a dict key
        """
        return "{}::{}".format(firstname.lower().strip(), lastname.lower().strip())
#
