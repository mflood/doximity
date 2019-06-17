"""
   friven_loader.py

   Manages a memory friendly Queue
   that holds user data from Friendly Vendor
"""
import logging
import os
import queue
import threading
from frivenmeld.loggingsetup import APP_LOGNAME
from frivenmeld.friendly_vendor.friendly_vendor_api import FriendlyVendorApi

USERS_PER_PAGE = 1000

class FrivenLoader(threading.Thread):
    """
        Maintains a Queue of FriendlyVender user objects
        Ordered by Last Name

        Grabs data from FriendlyVendorApi
    """
    def __init__(self, friven_api_url):
        self._friven_api = FriendlyVendorApi(api_url=friven_api_url)
        self._logger = logging.getLogger(APP_LOGNAME)
        self._user_queue = None
        self._thread_plunger = None
        self._page_range_start = 1
        self._page_range_end = None
        super(FrivenLoader, self).__init__()

    def _get_percentage_count(self, percent):
        """
            Queries the API for the number of
            pages and assuming USERS_PER_PAGE,
            returns the number representing
            PERCENT users from the entire
            collection.
        """
        assert 1 <= percent <= 100

        total_pages = self._friven_api.get_user_page_count()
        percent_as_float = percent / 100.0
        percentage_count = int(percent_as_float * total_pages * USERS_PER_PAGE)
        self._logger.debug("%s percent of (%s x %s) is %s",
                           percent,
                           total_pages,
                           USERS_PER_PAGE,
                           percentage_count)
        return percentage_count

    def set_page_range(self, first_page_number, last_page_number):
        """
            Just process a range of pages
        """
        self._page_range_start = int(first_page_number)

        if last_page_number:
            assert first_page_number <= last_page_number
            self._page_range_end = int(last_page_number)
        else:
            self._page_range_end = None

        self._logger.debug("FrivenLoader configured to process pages %s-%s",
                           self._page_range_start,
                           self._page_range_end)

    def init_queue_data_percent(self, percent):
        """
            Initializes the queue with a maxsize
            that is PERCENT % of the total number
            of user records that are available
            from Friendly Vendor
        """
        percentage_count = self._get_percentage_count(percent=percent)
        self.init_queue(maxsize=percentage_count)

    def init_queue(self, maxsize):
        """
            Instantiate the user_queue, setting
            the maxsize to restrict the amount of
            data we store locally
        """
        self._logger.info("FrivenLoader queue size set to %s", maxsize)
        self._user_queue = queue.Queue(maxsize=maxsize)

    def get_queue(self):
        """
            Accessor for _user_queue
        """
        return self._user_queue

    def stop(self):
        """
            Empties the _thread_plunger queue
            and drains all items from the user_queue
            so the run() method can exit gracefully
        """

        # we should only be calling this once
        assert not self._thread_plunger.empty()

        if self._thread_plunger:
            self._logger.info("Pulling the plug on FrivenLoader")
            self._thread_plunger.get(timeout=1)
            while True:
                try:
                    self._user_queue.get(timeout=1)
                except queue.Empty:
                    self._logger.info("FrivenLoader queue drained")
                    return

    def run(self):
        """
            Threading main function invoked
            when start() is called
        """

        assert self._user_queue is not None

        # Having a single item in this queue
        # is what keeps the run loop going
        self._thread_plunger = queue.Queue()
        self._thread_plunger.put("plug")

        # initialize the current_page.
        # usually this will be 1, but we allow
        # the start page to be configurable in the case
        # where we want to have a few of these running
        # on different machines, processing different
        # subsets of Friendly Vendor user data
        current_page = self._page_range_start

        while True:
            if self._thread_plunger.empty():
                self._logger.info("Someone pulled the plug on FrivenLoader. Bailing...")
                return

            # Grab a full page of data
            page, total_pages, users = self._friven_api.get_user_page(page_number=current_page)
            if users:
                self._logger.info("FrivenLoader processing %s users from page %s/%s %s-%s",
                                  len(users),
                                  page,
                                  total_pages,
                                  users[0]['lastname'],
                                  users[-1]['lastname'])

            else:
                self._logger.info("Page %s is empty. FrivenLoader finished getting users",
                                  page)
                break

            # put all the users on the queue
            # this will block if the user_queue
            # is maxed out
            for index, user in enumerate(users):
                user['friendly_vendor_page'] = page
                user['friendly_vendor_row'] = index + 1
                self._user_queue.put(user)

            current_page += 1

            # usually _page_range_end will be None,
            # but it is configurable if we want
            # to process a smaller window of user pages
            if self._page_range_end and current_page > self._page_range_end:
                break

# pylint: disable=invalid-name
if __name__ == "__main__":
    # Integration Testing
    from frivenmeld.loggingsetup import init_logging     # pragma: no cover
    init_logging(logging.INFO)                           # pragma: no cover

    api_url = os.getenv('FRIENDLY_VENNDOR_API_URL')      # pragma: no cover
    friven_loader = FrivenLoader(friven_api_url=api_url) # pragma: no cover
    #friven_loader.init_queue(maxsize=20000)             # pragma: no cover
    friven_loader.init_queue_data_percent(percent=10)    # pragma: no cover
    friven_loader.start()                                # pragma: no cover
    count = 0                                            # pragma: no cover
    while True:                                          # pragma: no cover
        friven_loader.get_queue().get(timeout=10)        # pragma: no cover
        count += 1                                       # pragma: no cover
        if count == 100:                                 # pragma: no cover
            # simulate early temination
            friven_loader.stop()                         # pragma: no cover
            break                                        # pragma: no cover

    print("Waiting for friven_loader to stop")           # pragma: no cover
#
