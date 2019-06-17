"""
    test friendly_vendor_api
"""
import pytest
import requests
# pylint: disable=import-error
import requests_mock
from frivenmeld.friendly_vendor.friendly_vendor_api import FriendlyVendorApi
from frivenmeld.friendly_vendor.friendly_vendor_api import FriendlyVendorApiError
from frivenmeld.friendly_vendor.friendly_vendor_api import EMPTY_PAGE_NUMBER

TEST_API_URL = "https://cute.sm/vi"

@pytest.fixture()
def req_mock():
    """
        # info on requests mocking
        # usually requests_mock would be available as fixture
        https://requests-mock.readthedocs.io/en/latest/pytest.html
        # but requests_mock not available as fixture in python 3
        https://github.com/pytest-dev/pytest/issues/2749
        # hence, this fixture
    """
    with requests_mock.Mocker() as the_mocker:
        yield the_mocker

@pytest.fixture()
def friven():
    """
        Fixture of an instantiaed FriendlyVendorApi object
    """
    friv = FriendlyVendorApi(api_url=TEST_API_URL)
    return friv

@pytest.fixture()
def response_page_three():
    """
        A healthy text response
        for page 3
    """
    raw_text = """{
  "current_page": 3,
  "total_pages": 76,
  "users": [
    {
      "firstname": "Melanie",
      "id": 2360,
      "last_active_date": "2017-02-01",
      "lastname": "Baughman",
      "practice_location": "black",
      "specialty": "Cardiology",
      "user_type_classification": "Leader"
    },
    {
      "firstname": "Jerry",
      "id": 6746,
      "last_active_date": "2017-01-12",
      "lastname": "Baughman",
      "practice_location": "boaz",
      "specialty": "Orthopedics",
      "user_type_classification": "Lurker"
    }]}"""

    return raw_text


# pylint: disable=redefined-outer-name
def test_repr(friven):
    """
        test __repr__()
    """
    repr(friven)

def test_get_user_page(req_mock, friven, response_page_three):
    """
        tests get_user_page()
    """

    mock_url = friven.get_user_url(page_number=3)

    # this sets up the mock_urtl with a mock response
    req_mock.get(mock_url, text=response_page_three)

    current_page, total_pages, users = friven.get_user_page(page_number=3)

    assert current_page == 3
    assert total_pages == 76
    assert len(users) == 2

def test_get_user_page_count(req_mock, friven, response_page_three):
    """
        tests get_user_page_count()
    """

    mock_url = friven.get_user_url(page_number=EMPTY_PAGE_NUMBER)

    # this sets up the mock_urtl with a mock response
    req_mock.get(mock_url, text=response_page_three)

    total_pages = friven.get_user_page_count()

    assert total_pages == 76

def test_connection_error(req_mock, friven):
    """
        mocks a ConnectionError
    """

    mock_url = friven.get_user_url(page_number=3)

    # this sets up the maildog api_url with a mock response
    req_mock.get(mock_url, exc=requests.exceptions.ConnectionError)

    with pytest.raises(FriendlyVendorApiError):
        friven.get_user_page(page_number=3)

def test_unexpected_error(req_mock, friven):
    """
        Triggers the generic Exception branch
    """

    mock_url = friven.get_user_url(page_number=3)

    # this sets up the maildog api_url with a mock response
    req_mock.get(mock_url, exc=ValueError)

    with pytest.raises(FriendlyVendorApiError):
        friven.get_user_page(page_number=3)

def test_404(req_mock, friven):
    """
        Mocks a 404 response
    """

    mock_url = friven.get_user_url(page_number=3)

    # this sets up the maildog api_url with a mock response
    req_mock.get(mock_url, text="who cares", status_code=404)

    with pytest.raises(FriendlyVendorApiError):
        friven.get_user_page(page_number=3)
