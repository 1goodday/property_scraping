# Importing the required modules
import time
import random
import requests
import xml.etree.ElementTree as ET
import sys
from bs4 import BeautifulSoup
import db_functions as DBF
import property_info as PI


# Finds the urls starting from url0 and returning them in url_list (a Python list type of strings).
# URLs are extracted from main pages in multiples of 120, but only max_urls number of them are returned.
def fetch_urls_from_main_page(db_user, db_pw, db_host, db_name, table_name, url0, max_urls):
    page_cnt = 0
    url_list = []
    url = url0

    while len(url_list) < max_urls:
        html = requests.get(url)
        html_soup = BeautifulSoup(html.text, 'html.parser')

        for li in html_soup.find_all('li'):
            for a in li.find_all('a'):
                if a.get('class') == ['result-title', 'hdrlnk']:
                    url_list.append(a.get('href'))

        page_cnt += 1
        url = url0 + '?s=' + str(page_cnt * 120)
        time.sleep(2*random.random())

    return url_list[:max_urls]


# Finds the urls starting from url0 and returning them in url_list (a Python list type of strings).
# Maximum number of pages that search is done in them is specified by max_pages. Each page includes 25 urls.
# RSS is removed from craigslist recently and this function is unusable.
def fetch_urls_from_rss(db_user, db_pw, db_host, db_name, table_name, url0, max_pages):
    page_cnt = 0
    url_list = []

    while page_cnt < max_pages:
        url = url0 + '&s=' + str(page_cnt*25)
        root = ET.fromstring((requests.get(url)).content)

        for child in list(root):
            if child.items()[0][1].find(url0) == -1:
                url_list.append(child.items()[0][1])

        page_cnt += 1
        time.sleep(2*random.random())

    return url_list


# Input parameters

# Password of local DB
db_pw = 'x'
# Name of local DB
db_name = 'learn'
# Table name to store fetched data
table_name = 'vcl'
# First page address
url0 = 'https://vancouver.craigslist.org/search/apa'
# Number of urls to extract from main page to be added to the DB along with their property infos.
max_urls = 20
# DB location and user
db_user = 'root'
db_host = 'localhost'

# table fields:
# Field           | Type                 | Null | Key | Default | Extra |
# +-----------------+----------------------+------+-----+---------+-------+
# | url             | varchar(200)         | YES  |     | NULL    |       |
# | post_id         | varchar(20)          | YES  |     | NULL    |       |
# | prop_title      | varchar(200)         | YES  |     | NULL    |       |
# | posted_date     | datetime             | YES  |     | NULL    |       |
# | updated_date    | datetime             | YES  |     | NULL    |       |
# | available_date  | datetime             | YES  |     | NULL    |       |
# | prop_latitude   | decimal(9,6)         | YES  |     | NULL    |       |
# | prop_longitude  | decimal(9,6)         | YES  |     | NULL    |       |
# | sqft            | mediumint(9)         | YES  |     | NULL    |       |
# | no_of_bedrooms  | tinyint(3) unsigned  | YES  |     | NULL    |       |
# | no_of_bathrooms | tinyint(3) unsigned  | YES  |     | NULL    |       |
# | prop_rental     | smallint(5) unsigned | YES  |     | NULL    |       |
# | no_of_images    | tinyint(3) unsigned  | YES  |     | NULL    |       |

table_fields = ("url", "post_id", "prop_title", "posted_date", "updated_date", "available_date", "prop_latitude",
                "prop_longitude", "sqft", "no_of_bedrooms", "no_of_bathrooms", "prop_rental", "no_of_images")
table_types = ("varchar(200)", "varchar(20)", "varchar(200)", "datetime", "datetime", "datetime", "decimal(9,6)",
               "decimal(9,6)", "mediumint(9)", "tinyint(3) unsigned", "tinyint(3) unsigned", "smallint(5) unsigned", "tinyint(3) unsigned")

# Creates the DB.
try:
    DBF.create_table(db_user, db_pw, db_host, db_name,
                 table_name, table_fields, table_types)
except:
    print('There was an error creating the "%s" table or table already exists.\n' % table_name)

# Fetches the current urls stored in the DB.
urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
print('%i items already exist in the DB.\n' % len(urls_from_db))

# Fetches a number of urls from the main page as specified by max_urls and compares this list with current DB to identify the new urls and writes them to the DB along with with their extracted property infos.
urls_from_main = fetch_urls_from_main_page(
    db_user, db_pw, db_host, db_name, table_name, url0, max_urls)
new_urls = list(set(urls_from_main) - set(urls_from_db))

rows = []

print("Extracting property information of new URLs.")
print("Please be patient. Sleep times are considered when fetching the pages not to overload the website server.")

for new_url in new_urls:
    new_rental_property = PI.rental_property(new_url)
    property_info_of_new_url = (new_rental_property.url,
                                new_rental_property.get_post_id(),
                                new_rental_property.get_prop_title(),
                                new_rental_property.get_posted_date(),
                                new_rental_property.get_updated_date(),
                                new_rental_property.get_available_date(),
                                new_rental_property.get_prop_latitude(),
                                new_rental_property.get_prop_longitude(),
                                new_rental_property.get_sqft(),
                                new_rental_property.get_no_of_bedrooms(),
                                new_rental_property.get_no_of_bathrooms(),
                                new_rental_property.get_prop_rental(),
                                new_rental_property.get_no_of_images()
                                )
    rows += [property_info_of_new_url]
    time.sleep(2*random.random())

    progress = 100 * len(rows) / max_urls
    sys.stdout.write('\r')
    sys.stdout.write("[%-20s] %d%%" % ('='*int(20*progress/100), progress))
    sys.stdout.flush()

if rows:
    DBF.write_rows_to_db(db_user, db_pw, db_host, db_name, table_name, rows)
else:
    print('No new items were found.')

# Cleans the DB based on "if two items have the same post_id, keep the one with the most recent updated_date" criterion.
print("A little cleaning up the DB ...")
pre_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
rmv_dup_field = 'post_id'
rmv_dup_decide_field = 'updated_date'
DBF.clean_db_keep_updated(db_user, db_pw, db_host, db_name,
                          table_name, rmv_dup_field, rmv_dup_decide_field)
post_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
print('%i items removed from the DB based on "if two items have the same post_id, keep the one with the most recent updated_date" criteria.' %
      (len(pre_urls_from_db) - len(post_urls_from_db)))

# Cleans the DB based on "if two items have the same post_id, keep the one with the most recent posted_date" criterion.
pre_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
rmv_dup_decide_field = 'posted_date'
DBF.clean_db_keep_updated(db_user, db_pw, db_host, db_name,
                          table_name, rmv_dup_field, rmv_dup_decide_field)
post_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
print('%i items removed from the DB based on "if two items have the same post_id, keep the one with the most recent posted_date" criteria.' %
      (len(pre_urls_from_db) - len(post_urls_from_db)))

# Cleans the DB based on "post_id is NULL" criterion.
pre_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
null_field = 'post_id'
DBF.clean_db_delete_null(db_user, db_pw, db_host, db_name,
                         table_name, null_field)
post_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
print('%i items removed from the DB based on "post_id is NULL" criteria.' %
      (len(pre_urls_from_db) - len(post_urls_from_db)))

# cleans the DB based on "duplicate rows" criterion.
pre_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
DBF.clean_db_remove_duplicate_rows(
    db_user, db_pw, db_host, db_name, table_name)
post_urls_from_db = DBF.fetch_column_from_db(
    db_user, db_pw, db_host, db_name, table_name, "url")
print('%i items removed from the DB based on "duplicate rows" criteria.' %
      (len(pre_urls_from_db) - len(post_urls_from_db)))