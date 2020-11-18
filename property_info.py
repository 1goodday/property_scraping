import requests
from bs4 import BeautifulSoup
from datetime import datetime


class rental_property:
    def __init__(self, url):
        self.url = url

        self.html = requests.get(self.url)
        self.html_soup = BeautifulSoup(self.html.text, 'html.parser')

        if 'craigslist.org' in self.url:
            self.web_site = 'craigslist.org'
        else:
            self.web_site = 'unknown_web_site'

    def get_post_id(self):
        for div in self.html_soup.find_all('div'):
            if div.get('class') == ['postinginfos']:
                for p in div.find_all('p'):
                    if p.get('class') == ['postinginfo']:
                        if 'post id: ' in p.get_text():
                            post_id = p.get_text().replace('post id: ', '')

        try:
            post_id
        except NameError:
            post_id = None

        return post_id

    def get_prop_title(self):
        for head in self.html_soup.find_all('head'):
            for meta in head.find_all('meta'):
                try:
                    if meta['property'] == 'og:title':
                        prop_title = meta['content']
                except:
                    pass

        try:
            prop_title
        except NameError:
            prop_title = None

        return prop_title

    def get_posted_date(self):
        for div in self.html_soup.find_all('div'):
            if div.get('class') == ['postinginfos']:
                for p in div.find_all('p'):
                    if p.get('class') == ['postinginfo', 'reveal']:
                        if 'posted: ' in p.get_text():
                            posted_date = datetime.strptime(
                                p.get_text().replace('posted: ', ''), '%Y-%m-%d %H:%M')

        try:
            posted_date
        except NameError:
            posted_date = None

        return posted_date

    def get_updated_date(self):
        for div in self.html_soup.find_all('div'):
            if div.get('class') == ['postinginfos']:
                for p in div.find_all('p'):
                    if p.get('class') == ['postinginfo', 'reveal']:
                        if 'updated: ' in p.get_text():
                            updated_date = datetime.strptime(
                                p.get_text().replace('updated: ', ''), '%Y-%m-%d %H:%M')

        try:
            updated_date
        except NameError:
            updated_date = None

        return updated_date

    def get_available_date(self):
        for p in self.html_soup.find_all('p'):
            for span in p.find_all('span'):
                try:
                    available_date = datetime.strptime(
                        span['data-date'], '%Y-%m-%d')
                except:
                    pass

        try:
            available_date
        except NameError:
            available_date = None

        return available_date

    def get_prop_latitude(self):
        for div in self.html_soup.find_all('div'):
            try:
                prop_latitude = div['data-latitude']
            except:
                pass

        try:
            prop_latitude
        except NameError:
            prop_latitude = None

        return prop_latitude

    def get_prop_longitude(self):
        for div in self.html_soup.find_all('div'):
            try:
                prop_longitude = div['data-longitude']
            except:
                pass

        try:
            prop_longitude
        except NameError:
            prop_longitude = None

        return prop_longitude

    def get_sqft(self):
        try:
            for p in self.html_soup.find_all('p'):
                if p.get('class') == ['attrgroup']:
                    for span in p.find_all('span'):
                        if span.get('class') == ['shared-line-bubble']:
                            if 'ft2' in span.get_text():
                                sqft = span.get_text().replace('ft2', '')
        except:
            pass

        try:
            sqft
        except NameError:
            sqft = None

        return sqft

    def get_no_of_bedrooms(self):
        try:
            for p in self.html_soup.find_all('p'):
                if p.get('class') == ['attrgroup']:
                    for span in p.find_all('span'):
                        if span.get('class') == ['shared-line-bubble']:
                            for b in span.find_all('b'):
                                if 'BR' in b.get_text():
                                    no_of_bedrooms = b.get_text().replace('BR', '')
        except:
            pass

        try:
            no_of_bedrooms
        except:
            no_of_bedrooms = None

        return no_of_bedrooms

    def get_no_of_bathrooms(self):
        try:
            for p in self.html_soup.find_all('p'):
                if p.get('class') == ['attrgroup']:
                    for span in p.find_all('span'):
                        if span.get('class') == ['shared-line-bubble']:
                            for b in span.find_all('b'):
                                if 'Ba' in b.get_text():
                                    no_of_bathrooms = b.get_text().replace('Ba', '')
        except:
            pass

        try:
            no_of_bathrooms
        except:
            no_of_bathrooms = None

        return no_of_bathrooms

    def get_prop_rental(self):
        for h1 in self.html_soup.find_all('h1'):
            for span in h1.find_all('span'):
                try:
                    if span['class'][0] == 'price':
                        prop_rental = str(span.string).replace('$', '')
                        prop_rental = prop_rental.replace(',', '')
                except:
                    pass

        try:
            prop_rental
        except NameError:
            prop_rental = None

        return prop_rental

    def get_no_of_images(self):
        for span in self.html_soup.find_all('span'):
            try:
                if span.string.find('image 1 of ') == 0:
                    no_of_images = span.string.replace('image 1 of ', '')
            except:
                pass

        try:
            no_of_images
        except NameError:
            no_of_images = None

        return no_of_images