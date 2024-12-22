"""
this module contains common constants used across different scripts
in lenta module
"""
from scrapers.common import headers

# this name is used to retrieve supermarket_id from the database
SUPERMARKET_NAME = 'Лента'


# these headeres are both for catalogue and scraper scripts
# the SessionToken is for the address: Томск, Елизаровых ул., 13
headers.update({
    'SessionToken': '00BAEB8C9C2BCBE317F8340311FD1E74',
    'DeviceID': '1',
    'X-Retail-Brand': 'lo',
    'X-Platform': 'omniweb',
    'X-Domain': 'tomsk',
    'X-Delivery-Mode': 'pickup',
})
