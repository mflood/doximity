"""
    melder.py

    Pulls users from the Mysql Queueu
    and the Friendly Vendor Queue
    and sends them to the combiner

    Assuming Users are coming out of each queue
    in order of lastname, it performs a merge
    operation that essentially builds groups
    of users with the same last name.

    It then sends each "lastname-group" to the
    combiner (CombiningEngine) for further processing.


    Example incoming data:
    mysql_queue  --> ['fred asbury', 'helen asbury', 'bob allen'] -->
    friven_queue --> ['june asbury', 'fred asbury', 'judd arkin', 'bob allen'] -->

    Only creates groups when the lastname is in both queues
    In this case, 'judd arkin' would be silently discarded

    So, the Melder reads the incoming data and outputs pairs of lists:


               MySQL (Doximity)               FRIVEM (Friendly Vendor)
               ----------------               ------------------------
    ['fred asbury', 'helen asbusy'] ------ ['june asbury', 'fred asbury']
                      ['bob allen'] ------- ['bob allen']


    Those pairs ^ are then sent to the Combiner.
    fyi the order of users in each sublist does not matter
    (i.e. they don't need to be sorted by first name)

"""
import logging
import queue
from frivenmeld.loggingsetup import APP_LOGNAME

# pylint: disable=too-many-arguments
# pylint: disable=too-few-public-methods
class Melder():
    """
        Combines users from Friendly Vendor and Doximity
    """

    def __init__(self,
                 friven_loader,
                 mysql_loader,
                 combining_engine,
                 friven_timeout,
                 mysql_timeout):
        self._logger = logging.getLogger(APP_LOGNAME)
        self._friven_loader = friven_loader
        self._mysql_loader = mysql_loader
        self._combining_engine = combining_engine
        self._friven_timeout = friven_timeout
        self._mysql_timeout = mysql_timeout

    def meld(self):
        """
            Grab users from Friendly Vendor
            and Doximity Mysql
            and group them by last name.

            Then send them to the Combining Engine
        """

        self._logger.info("Starting Meld")
        self._friven_loader.start()
        friven_queue = self._friven_loader.get_queue()

        # Grab the first friven user
        try:
            friven_user = friven_queue.get(timeout=self._friven_timeout)

        except queue.Empty:
            self._logger.info("There is no api data available.")
            return

        friven_lastname = friven_user['lastname'].lower().strip()
        self._logger.debug("first friven lastname is '%s'.",
                           friven_lastname)

        # use the lastname of the first friven_user
        # to determine where we start crawling the
        # mysql user data.
        # I verified that the lowercase nature
        # of this value does not affect the mysql
        # query for lastname.
        self._mysql_loader.set_initial_lastname(friven_lastname)
        self._mysql_loader.start()
        mysql_queue = self._mysql_loader.get_queue()

        # grab the first mysql user
        try:
            mysql_user = mysql_queue.get(timeout=self._mysql_timeout)
        except queue.Empty:
            self._logger.info("Timed out waiting for Doximity data. "
                              "Increase mysql_timeout.")
            return
        mysql_lastname = mysql_user['lastname'].lower().strip()
        self._logger.debug("First mysql lastname is '%s'",
                           mysql_lastname)

        while True:

            # both sources of data are sorted by lastname
            # so we can apply a merge technique
            # to zip the data together

            try:
                # if mysql_lastname is behind,
                # pull stuff off the queue until it matches or passes
                # what we have for friven_lastname
                while mysql_lastname < friven_lastname:
                    self._logger.debug("mysql '%s' < friven '%s'",
                                       mysql_lastname,
                                       friven_lastname)
                    mysql_user = mysql_queue.get(timeout=self._mysql_timeout)
                    mysql_lastname = mysql_user['lastname'].lower().strip()

                # if friven_lastname is behind,
                # pull stuff off the queue until it matches or passes
                # what we have for mysql_lastname
                while friven_lastname < mysql_lastname:
                    self._logger.debug("friven '%s' < mysql '%s'",
                                       friven_lastname,
                                       mysql_lastname)

                    friven_user = friven_queue.get(timeout=self._friven_timeout)
                    friven_lastname = friven_user['lastname'].lower().strip()

            except queue.Empty:
                self._logger.info("Timed out trying to find common last names")
                self._logger.info("Please wait a few seconds for things to wrap up.")
                # we're not finding anoy more matches.
                # we can drain the queues
                self._mysql_loader.stop()
                self._friven_loader.stop()
                break

            # Most likely we get here when we have common lastnames
            # otherwise we drop into the else clause when
            # we ran out of matching names
            if friven_lastname == mysql_lastname:

                # we will overwrite friven_lastname and mysql_lastname
                # as we pull stuff off the queue
                # so keep a reference to the  lastname
                # that got us here to begin with
                working_lastname = friven_lastname

                self._logger.debug("found common lastname: '%s'",
                                   working_lastname)

                # we'll store all the users with
                # this lastname in these two lists
                friven_list = []
                mysql_list = []

                # if one of the queues empties
                # we'll want to pull the plug
                # on the other one
                stop_the_madness = False

                try:
                    # pull off all friven matching last name
                    while friven_lastname == working_lastname:

                        friven_list.append(friven_user)
                        friven_user = friven_queue.get(timeout=self._friven_timeout)
                        friven_lastname = friven_user['lastname'].lower().strip()

                except queue.Empty:
                    self._logger.info("Timed out getting more friven data for '%s'",
                                      working_lastname)
                    self._logger.info("Please wait a few seconds for things to wrap up.")
                    stop_the_madness = True

                try:
                    # pull off all mysql matching last name
                    while mysql_lastname == working_lastname:

                        # yoyo: DRY this up
                        mysql_list.append(mysql_user)
                        mysql_user = mysql_queue.get(timeout=self._mysql_timeout)
                        mysql_lastname = mysql_user['lastname'].lower().strip()

                except queue.Empty:
                    self._logger.info("Timed out waiting for more mysql data for '%s'",
                                      working_lastname)
                    self._logger.info("Please wait a few seconds for things to wrap up.")
                    stop_the_madness = True

                # we have all the users with the current lastname
                # Combine the users from each system
                self._combine_users(friven_list=friven_list,
                                    mysql_list=mysql_list)

                if stop_the_madness:
                    self._mysql_loader.stop()
                    self._friven_loader.stop()
                    break

    def _combine_users(self, friven_list, mysql_list):
        """
            This is where the magic happens

            We have a list of Friven Users and a list
            of Mysql Users - all sharing the same last
            name.  We need to pair them up the best we can.

            We're gonna delegate this task to
            the Combiner a.k.a. CombiningEngine
        """
        self._combining_engine.combine(friven_user_list=friven_list, mysql_user_list=mysql_list)

# end
