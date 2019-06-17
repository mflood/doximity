import pytest
from unittest.mock import patch
import requests

from frivenmeld.friendly_vendor.friven_loader import FrivenLoader

import frivenmeld.friendly_vendor.friendly_vendor_api




def test_set_page_range():
    
    friven_loader = FrivenLoader(friven_api_url="http://dud")
    friven_loader.set_page_range(first_page_number=1,
                                 last_page_number=1)
    friven_loader.set_page_range(first_page_number=1,
                                 last_page_number=0)
 

def test_queue():
    
    friven_loader = FrivenLoader(friven_api_url="http://dud")
    friven_loader.init_queue(maxsize=200)
    friven_loader.get_queue()


def test_init_queue():
    
    friven_loader = FrivenLoader(friven_api_url="http://dud")

    with patch.object(frivenmeld.friendly_vendor.friendly_vendor_api.FriendlyVendorApi, 'get_user_page_count', return_value=5000) as mock_method:
        friven_loader.init_queue_data_percent(percent=12)

