"""
    friendly_vendor_api.py

    Module for querying Friendly Vendor API
"""
import logging
import requests
from frivenmeld.loggingsetup import APP_LOGNAME
from frivenmeld.loggingsetup import init_logging

# a page number that is so high
# that the api will return an empty
# user list
EMPTY_PAGE_NUMBER = 100000

class FriendlyVendorApiError(Exception):
    """
        Raised when things go south in this module
    """
    pass

class FriendlyVendorApi():
    """
        Object that makes calls to Friendly Vendor api
        and provides some related utility functions
    """

    def __init__(self, api_url):
        self._logger = logging.getLogger(APP_LOGNAME)
        self._api_url = api_url

    def __repr__(self):
        return "FriendlyVendorApi(api_url='{}')".format(self._api_url)

    def _get_api_url(self):
        """
            Returns the base URL used to hit the api
        """
        return self._api_url

    def get_user_url(self, page_number):
        """
            Returns the complete url for the
            '/users' endpoint for the given page_numnber
        """
        base_url = self._get_api_url().strip("/")
        return "{}/users?page={}".format(base_url, page_number)

    def get_user_page_count(self):
        """
            Returns the number of pages
            of users available
            from Friendly Vendor's api.
        """
        # query a page that does not exist, so we
        # can avoid pulling down data we will just ignore
        _, total_pages, _ = self.get_user_page(EMPTY_PAGE_NUMBER)
        self._logger.debug("There are %s pages available", total_pages)
        return int(total_pages)

    def get_user_page(self, page_number):
        """
            Hits Friendly Vendor API
            /users?page=page_number endpoint

            raises FriendlyVendorApiError
            for all errors

            returns page_number, total_pages, user_list
        """
        try:
            url = self.get_user_url(page_number=page_number)
            self._logger.info("%s", url)
            response = requests.get(url)
            self._logger.debug("response.headers: %s", response.headers)
            self._logger.debug("response.headers: %s", response.headers)
            # self._logger.debug("response.text: %s", response.text)

            if response.status_code != 200:
                error_message = ("Error hitting Friendly Vendor API (status_code {}): "
                                 "{}".format(response.status_code, response.text))
                self._logger.error(error_message)
                raise FriendlyVendorApiError(error_message)

            as_json = response.json()

            current_page = int(as_json.get("current_page"))
            total_pages = int(as_json.get("total_pages"))
            users = as_json.get("users")

            self._logger.debug("Current Page: %s", current_page)
            self._logger.debug("Total Pages: %s", total_pages)
            self._logger.debug("User Count: %s", len(users))

            return current_page, total_pages, users

        except requests.exceptions.ConnectionError as error:
            self._logger.error(error)
            raise FriendlyVendorApiError(error)

        except Exception as error:
            message = "Unexpected Exception: {}".format(error)
            self._logger.error(message)
            raise FriendlyVendorApiError(message)


# pylint: disable=invalid-name
if __name__ == "__main__":
    # Invoke this script directly for integration Test
    # and to look up a page / row from Friendly Vendor
    import pprint                                                       # pragma: no cover
    page = 152                                                          # pragma: no cover
    row = 677                                                           # pragma: no cover
    init_logging(logging.DEBUG)                                         # pragma: no cover
    API_URL = "http://de-tech-challenge-api.herokuapp.com/api/v1"       # pragma: no cover
    frivenapi = FriendlyVendorApi(api_url=API_URL)                      # pragma: no cover
    page, total, page_users = frivenapi.get_user_page(page_number=page) # pragma: no cover
    count = frivenapi.get_user_page_count()                             # pragma: no cover
    pprint.pprint(page_users[row - 1])                                  # pragma: no cover

# end
