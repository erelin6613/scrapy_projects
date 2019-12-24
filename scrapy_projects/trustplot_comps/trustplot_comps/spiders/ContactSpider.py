""" 
    Update from 24-Dec-2019

    Spider developed to scrape sites for Pissed Consumer;
    a good deal of functionality should be atributed to
    Vitaliy Bulah.

    Added task specific functionality to extract data
    needed to accomplish the task at hand (contacts 
    extraction for this spider)
"""

from scrapy import Spider
from scrapy import Request
import json
from .. import settings
from w3lib.http import basic_auth_header
import pandas as pd
from scrapy.selector import Selector
from bs4 import BeautifulSoup
import re
import time
import requests
import xml.etree.ElementTree as etree
import pyap
from urllib.parse import urlparse
from selenium import webdriver
from shutil import which
from scrapy_selenium import SeleniumRequest
from address_parser import Parser

address_parser = Parser()

#yield SeleniumRequest(url=url, callback=self.parse_result)

"""SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('/home/val/coding/chromedriver')
SELENIUM_DRIVER_ARGUMENTS=['-headless']"""

#options = webdriver.ChromeOptions()
#options.add_argument('headless')
#driver = webdriver.Chrome(executable_path = '/home/val/coding/chromedriver', options = options)

def fill_data_base(frame):
# Automatic filling the data base with scraped information

    connection = sqlite3.connect('./for_hirerush.db')
    cursor = connection.cursor()
    try:
        cursor.execute('CREATE TABLE for_hirerush (Name TEXT, LinkOnPlatform TEXT, Platform TEXT, Category TEXT, Overview TEXT, Website TEXT, Phone TEXT, YearInBusiness INTEGER, Rating DECIMAL, IsPaidCustomer INTEGER, Lisensed TEXT, City TEXT, State TEXT)')
    except:
        pass
    cursor.execute("INSERT INTO for_hirerush VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
        (frame['Name'], frame['LinkOnPlatform'], frame['Platform'], frame['Category'], frame['Overview'], frame['Website'], frame['Phone'], frame['Rating'], frame['Lisensed'], frame['City'], frame['State']))
    connection.commit()
    cursor.close()
    connection.close()

#frame = pd.Series()
# response.selector.xpath('//article').get()

#index = 1

#frame = pd.read_csv('/home/val/coding/international_companies/trustpilot_data.csv')
#encoding = 'unicode_escape'
data_base = pd.read_csv('/home/val/coding/scrapy_projects/companies_merged_results.csv', encoding = 'unicode_escape')
#company_dict = {}
#for row, col in data_base.iterrows():
#    company_dict[col] = row[col]
columns = data_base.columns

'''
Index(['Company name', 'Unnamed: 1', 'url', 'email', 'phones', 'fax',
       'address1', 'city', 'state', 'zip', 'facebook', 'pinterest',
       'instagram', 'twitter', 'youtube', 'linkedin', 'privacy_policy_url',
       'privacy_text', 'terms_and_conditions_url', 'terms_and_conditions_text',
       'faq_url', 'faq_text', 'delivery_policy_url', 'delivery_text',
       'return_policy_url', 'return_text', 'warranty_url', 'warranty_text'],
      dtype='object')

'''

abbrs = { 'Alaska': 'AK', 'Alabama': 'AL', 'Arkansas': 'AR', 'Arizona': 'AZ', 'California': 'CA', 
            'Colorado': 'CO', 'Connecticut': 'CT', 'District-of-Columbia': 'DC', 'Delaware': 'DE', 
            'Florida': 'FL', 'Georgia': 'GA', 'Iowa': 'IA', 'Idaho':    'ID', 
            'Illinois': 'IL', 'Indiana': 'IN', 'Kansas': 'KS', 'Kentucky': 'KY', 
            'Louisiana': 'LA', 'Massachusetts': 'MA', 'Maryland':   'MD', 'Maine': 'ME', 
            'Michigan': 'MI', 'Minnesota': 'MN', 'Missouri': 'MO', 'Mississippi': 'MS',
            'Montana': 'MT', 'North-Carolina': 'NC', 'North-Dakota': 'ND', 'Nebraska': 'NE', 
            'New-Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'Nevada': 'NV', 
            'New-York': 'NY', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR',
            'Pennsylvania': 'PA', 'Rhode-Island': 'RI', 'South-Carolina': 'SC', 'South-Dakota': 'SD', 
            'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Virginia': 'VA', 
            'Vermont': 'VT', 'Washington': 'WA', 'Wisconsin': 'WI', 'West Virginia': 'WV',
            'Wyoming': 'WY'}

cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix','Philadelphia', 'San Antonio', 'San Diego', 
            'Dallas', 'San Jose',  'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'San Francisco', 'Charlotte', 
            'Indianapolis', 'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Detroit', 'Nashville', 
            'Portland', 'Memphis', 'Oklahoma City', 'Las Vegas', 'Louisville', 'Baltimore', 'Milwaukee', 
            'Albuquerque', 'Tucson', 'Fresno', 'Mesa', 'Sacramento', 'Atlanta', 'Kansas City', 'Colorado Springs', 
            'Miami', 'Raleigh', 'Omaha', 'Long Beach', 'Virginia Beach', 'Oakland', 'Minneapolis', 'Tulsa', 
            'Arlington', 'Tampa', 'New Orleans', 'Wichita', 'Cleveland', 'Bakersfield', 'Aurora', 'Anaheim', 
            'Honolulu', 'Santa Ana', 'Riverside', 'Corpus Christi', 'Lexington', 'Stockton', 'Henderson', 
            'Saint Paul', 'St. Louis', 'Cincinnati', 'Pittsburgh', 'Greensboro', 'Anchorage', 'Plano', 'Lincoln', 
            'Orlando', 'Irvine', 'Newark', 'Toledo', 'Durham', 'Chula Vista', 'Fort Wayne', 'Jersey City',
            'St. Petersburg', 'Laredo', 'Madison', 'Chandler', 'Buffalo', 'Lubbock', 'Scottsdale', 'Reno', 
            'Glendale', 'Gilbert', 'Winston–Salem', 'North Las Vegas', 'Norfolk', 'Chesapeake', 'Garland', 'Irving', 
            'Hialeah', 'Fremont', 'Boise', 'Richmond', 'Baton Rouge', 'Spokane', 'Des Moines', 'Tacoma', 
            'San Bernardino', 'Modesto', 'Fontana', 'Santa Clarita', 'Birmingham', 'Oxnard', 'Fayetteville', 
            'Moreno Valley', 'Rochester', 'Glendale', 'Huntington Beach', 'Salt Lake City', 'Grand Rapids', 
            'Amarillo', 'Yonkers', 'Aurora', 'Montgomery', 'Akron', 'Little Rock', 'Huntsville', 'Augusta', 
            'Port St. Lucie', 'Grand Prairie', 'Columbus', 'Tallahassee', 'Overland Park', 'Tempe', 'McKinney', 
            'Cape Coral', 'Shreveport', 'Frisco', 'Knoxville', 'Worcester', 'Brownsville', 'Vancouver', 
            'Fort Lauderdale', 'Sioux Falls', 'Ontario', 'Chattanooga', 'Providence', 'Newport News', 
            'Rancho Cucamonga', 'Santa Rosa', 'Oceanside', 'Salem', 'Elk Grove', 'Garden Grove', 'Pembroke Pines', 
            'Eugene', 'Oregon', 'Corona', 'Cary', 'Springfield','Fort Collins','Jackson','Alexandria', 'Hayward',
            'Lancaster', 'Lakewood', 'Clarksville', 'Palmdale', 'Salinas', 'Springfield', 'Hollywood', 'Pasadena', 
            'Sunnyvale', 'Macon', 'Kansas City', 'Pomona', 'Escondido', 'Killeen', 'Naperville', 'Joliet', 
            'Bellevue', 'Rockford', 'Savannah', 'Paterson', 'Torrance', 'Bridgeport', 'McAllen', 'Mesquite', 
            'Syracuse', 'Midland', 'Pasadena', 'Murfreesboro', 'Miramar', 'Dayton', 'Fullerton', 'Olathe', 
            'Orange', 'Thornton', 'Roseville', 'Denton', 'Waco', 'Surprise', 'Carrollton', 'West Valley City', 
            'Charleston', 'Warren', 'Hampton', 'Gainesville', 'Visalia', 'Coral Springs',  'Columbia', 
            'Cedar Rapids', 'Sterling Heights', 'New Haven', 'Stamford', 'Concord', 'Kent ', 'Santa Clara', 
            'Elizabeth', 'Round Rock', 'Thousand Oaks', 'Lafayette', 'Athens', 'Topeka', 'Simi Valley', 'Fargo', 
            'Norman', 'Columbia', 'Abilene', 'Wilmington', 'Hartford', 'Victorville', 'Pearland', 'Vallejo', 
            'Ann Arbor', 'Berkeley', 'Allentown', 'Richardson', 'Odessa', 'Arvada', 'Cambridge', 'Sugar Land', 
            'Beaumont', 'Lansing', 'Evansville', 'Rochester', 'Independence', 'Fairfield', 'Provo', 'Clearwater', 
            'College Station', 'West Jordan', 'Carlsbad', 'El Monte', 'Murrieta', 'Temecula', 'Springfield', 
            'Palm Bay', 'Costa Mesa', 'Westminster', 'North Charleston', 'Miami Gardens', 'Manchester', 'High Point', 
            'Downey', 'Clovis', 'Pompano Beach', 'Pueblo', 'Elgin', 'Lowell', 'Antioch', 'West Palm Beach', 'Peoria', 
            'Everett', 'Ventura', 'Centennial', 'Lakeland', 'Gresham', 'Richmond', 'Billings', 'Inglewood', 
            'Broken Arrow', 'Sandy Springs', 'Jurupa Valley', 'Hillsboro', 'Waterbury', 'Santa Maria', 'Boulder', 
            'Greeley', 'Daly City', 'Meridian', 'Lewisville', 'Davie', 'West Covina', 'League City', 'Tyler', 
            'Norwalk', 'San Mateo', 'Green Bay', 'Wichita Falls', 'Sparks', 'Lakewood', 'Burbank', 'Rialto', 'Allen', 
            'El Cajon', 'Las Cruces', 'Renton', 'Davenport', 'South Bend', 'Vista', 'Tuscaloosa', 'Clinton', 'Edison', 
            'Woodbridge', 'San Angelo', 'Kenosha', 'Vacaville']


class BaseSpider(Spider):

    name = 'contact'

    def __init__(self):
        super(Spider, self).__init__()
        self.urls = self._load_urls()
        #self.urls = []
        self.java_script = False

    def _load_urls(self):
        #data_base = pd.read_csv('/home/val/coding/scrapy_projects/usa_gov_base.csv')
        links = []
        link_columns = ['contact_link']
        for col in link_columns:
            for each in data_base[col]:
                try:
                    assert len(each) > 0
                    links.append(each)
                except Exception:
                    pass
        return links

    def start_requests(self):
     
        for url in self.urls:
            request = SeleniumRequest(url=url, callback=self.parse, meta={'splash': {'endpoint': 'render.html', 
                                                                            'args': {'html': 1,
                                                                                    'png': 1,
                                                                                    'width': 600,
                                                                                    'render_all': 1,
                                                                                    'wait': 0.5}}})
            yield request

    def parse(self, response):
        data_frame = pd.DataFrame()

        internal_links = {'contact_link': r'contact.*', 
                            'privacy_link': r'privacy.*policy', 
                            'delivery_link': r'(deliver|ship).*(policy)*', 
                            'terms_link': r'term.*(condition|use|service)', 
                            'faq_link': r'(faq)|(frequently.*asked.*question)', 
                            'return_link': r'return.*', 
                            'warranty_link': r'(warrant)|(certif)|(guarant)'}

        external_links = {'twitter': 'twitter.com', 
                            'facebook': 'facebook.com', 
                            'pinterest':'pinterest.com', 
                            'youtube': 'youtube.com',
                            'linkedin': 'linkedin.com'}
        #print(columns)

        def find_phones(response, contact_link):

            soup = BeautifulSoup(response.body, 'lxml')

            pattern_phones = [r'[(]?\d\d\d[)]?[-.*, ]?\d\d\d[-.*, ]?\d\d\d\d', 
                                r'+\d[(]?\d\d\d[)]?[-.*, ]?\d\d\d[-.*, ]?\d\d\d\d']
            phones = []
            for each in soup.get_text().split('\n'):

                for pattern in pattern_phones:
                    try:
                        phone_re = re.search(pattern, each)
                        if len(phone_re.group(0)) > 9 and len(phone_re.group(0)) < 15:
                            phones.append(phone_re.group(0))
                    except Exception as e:
                        pass
                        #print(e)
            str_phones = ''
            for phone in phones:
                if len(phones) > 1:
                    str_phones += phone+'; '

            return str_phones

        def find_address(responce, contact_link):
            soup = BeautifulSoup(response.body, 'lxml')
            for script in soup(["script", "style"]):
                script.extract()
            #text = soup.get_text().split('\n')
            try:
                address = str(pyap.parse(soup.text, country='US')[0])

            except Exception as e:
                #print(e)
                address = None
            try:
                assert len(contact_link) > 0
                if len(address) == 0:
                    request = SeleniumRequest(url=contact_link, callback=find_address, meta={'splash': {'endpoint': 'render.html', 
                                                                                                    'args': {'html': 1,
                                                                                                            'png': 1,
                                                                                                            'width': 600,
                                                                                                            'render_all': 1,
                                                                                                            'wait': 0.5}}})
            except Exception as e:
                #print(e)
                pass

            try:
                return address
            except Exception:
                return None

        def find_email(responce, contact_link):
            email_pattern = '[A-Za-z0-9]*@{1}[A-Za-z0-9]*\.(com|org|de|edu|gov|uk){1}'
            #email_pattern = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'

            soup = BeautifulSoup(response.body, 'lxml')
            for script in soup(["script", "style"]):
                script.extract()
            #text = soup.get_text().split('\n')
            for each in soup.get_text().split('\n'):
                try:
                    email_re = re.search(email_pattern, each)
                    if len(email_re.group(0)) > 5 and len(email_re.group(0)) < 75:
                        email = email_re.group(0)

                except Exception as e:
                    try:
                        assert len(contact_link) > 0
                        if len(email) == 0:
                            request = SeleniumRequest.follow(url=contact_link, callback=find_email, meta={'splash': {'endpoint': 'render.html', 
                                                                                                                    'args': {'html': 1,
                                                                                                                            'png': 1,
                                                                                                                            'width': 600,
                                                                                                                            'render_all': 1,
                                                                                                                            'wait': 0.5}}})
                    except Exception as e:
                        #print(e)
                        email = None
            try:
                return email
            except Exception:
                return None


        def find_name(responce, contact_link):
            metas = ['@property="og:site_name"', '@property="og:title"']
            for meta in metas:
                try:
                    name = response.xpath('//meta[{}]/@content'.format(meta)).getall()[0]
                except Exception:
                    pass

            try:
                assert len(name) > 0
                name = response.xpath('/tittle').getall()[0]
            except Exception:
                name = None

            return name

        def validate_link(link, current_url):

            discovered_bugs = {'/#/': '/', '.com/@': '.com/', '.org/@': '.org/','.edu/@': '.edu/', 
                                '.uk/@': '.uk/', '.com#': '.com/', '.org#': '.org/', 
                                '.edu#': '.edu/', '.uk#': '.uk/', '.com//': '.com/', 
                                '.org//': '.org/', '.edu//': '.edu/', '.uk//': '.uk/'}

            if link.startswith('https://') == True or link.startswith('http://') == True or link.startswith('www.') == True:
                return link

            else:
                if link.startswith('/') == True:
                    return current_url+link.replace('//', '/')

            for bug in discovered_bugs.keys():
                if bug in link:
                    link = link.replace(bug, discovered_bugs[bug])

        def find_links(response):

            links = {}
            soup = BeautifulSoup(response.body, 'lxml')
            for each in soup.find_all('a'):
                for ext_key, ext_val in external_links.items():
                    if ext_val in str(each.get('href')):
                        links[ext_key] = validate_link(str(each.get('href')), current_url)
                for int_key, int_val in internal_links.items():
                    try:
                        url = re.findall(int_val, each.get('href'))
                        if len(url) > 0:
                            links[int_key] = validate_link(str(each.get('href')), current_url)
                    except Exception as e:
                        pass
            for each in external_links.keys():
                try:
                    assert len(links[each]) > 0
                except Exception:
                    links[each] = None
            for each in internal_links.keys():
                try:
                    assert len(links[each]) > 0
                except Exception:
                    links[each] = None

            return links

        frame = pd.Series()

        print(response.status, '---', response.url)

        if response.status == 200:   

            current_url = response.url
            for url in response.xpath('//a/@href').re(r'.+?complaint.+?'):
                if url.__contains__('http'):
                    complaint_link = url
                else:
                    if url.startswith('/'):
                        complaint_link = current_url+url
                    else:
                        complaint_link = url

            for url in response.xpath('//a/@href').re(r'.+?contact.+?'):
                if url.__contains__('http'):
                    contact_link = url
                else:
                    if url.startswith('/'):
                        contact_link = current_url+url
                    else:
                        contact_link = url
            try:
                assert len(contact_link) > 0
            except Exception:
                contact_link = ''

            frame['url'] = current_url
            frame['phones'] = find_phones(response, contact_link)
            frame['address'] = find_address(response, contact_link)
            frame['email'] = find_email(response, contact_link)
            try:
                address = address_parser.parse(frame['address'])
            except Exception:
                pass
            try:
                frame['city'] = address.locality.city
            except Exception as e:
                #print(e)
                frame['city'] = None
            try:
                frame['state'] = address.locality.state
            except Exception:
                frame['state'] = None
            try:
                frame['zip'] = address.locality.zip
            except Exception:
                frame['zip'] = None
            for key in external_links.keys():
                try:
                    frame[key] = find_links(response)[key]
                except:
                    frame[key] = None
            for key in internal_links.keys():
                try:
                    frame[key] = find_links(response)[key]
                except:
                    frame[key] = None

        else:
            frame['url'] = current_url

        print(frame)
        data_frame = data_frame.append(frame, ignore_index=True)
        data_frame.to_csv('companies_merged_results-contacts.csv', header=False, mode='a')


    def _get_url(self, responce):
        self._logger.info(responce.url)


    def _remove_chars(self, string):
        if len(string) == 0:
            return string

        for i in range(2):
            if len(string) == 0:
                return ''
            if string[0] == '\n' or string[0] == '\r':
                string = string[1:]
            if len(string) == 0:
                return ''
            if string[-1] == '\n' or string[-1] == '\r':
                string = string[:-1]

        string = string.replace('\n', ';')

        string = self._remove_unicode_chars(string)

        return string.strip(' ')



    def _remove_unicode_chars(self, string):
        chars = [u'\xe2', u'\uFFFD', u'\xa0', u'\xf1', u'\u2013', u'\u2026',
                 u'\ufffd', u'\u2019', u'\xae', u'\xe9', u'\xc2', u'\u201d',
                 u'\u30a2', u'\u30e1', u'\u30ea', u'\u30ab', u'\u5408', u'\u8846',
                 u'\u56fd', u'\u200e', u'\xe9', u'\u201c', u'\xa9'
                 u'\ufffd', u'\u2018', u'\u2022', u'\xc0']
        for c in chars:
            string = string.replace(c, ' ')
        #print(string)
        return string


def replace_numbers(string):
    numbers = { 'hundred': 100, 'ninety':90, 'eighty': 80, 'seventy': 70, 'sixty': 60, 'fifty': 50, 'forty': 40, 
                'thirty': 30, 'twenty': 20, 'ten': 10, 'nine': 9, 'eight': 8, 'seven': 7, 'six': 6, 'five': 5, 
                'four': 4, 'three': 3, 'two': 2, 'one': 1, 'zero': 0}

    for key in numbers.keys():
        try:
            if key in string:
                string.replace(key, numbers[key])
        except Exception:
            pass

    return string

def numerize_string(string):
    num_string = ''
    nums_in_phone = 0
    #allowed_sym = 
    #article = soup.find('article')
    for character in string:
        if character.isalpha() == False:
            num_string += character
        if character.isnumeric() == True:
            nums_in_phone += 1
    if nums_in_phone > 6 and nums_in_phone <11:
        return num_string
    else:
        return None



"""            try:
                frame['name_2'] = soup.find('meta', attrs={'property':'og:site_name'}).get('content').strip()
            except Exception:
                try:
                    frame['name_2'] = soup.find('meta', attrs={'property':'og:title'}).get('content').strip()
                except Exception:
                    frame['name_2'] = soup.find('tittle').get_text()
            delims = ['|', '-', '/', ':']
            drop_words = ['home', 'homepage', 'home page', 'welcome', '.com', '.org']
            for delim in delims:
                if delim in frame['name_2']:
                    try:
                        for each in frame['name_2'].split(delim):
                            if drop_words[0] not in each.lower() and drop_words[1] not in each.lower() \
                            and drop_words[2] not in each.lower() and drop_words[3] not in each.lower() \
                            and drop_words[4] not in each.lower() and drop_words[5] not in each.lower():
                                frame['name_2'] = each
                    except Exception:
                        pass"""



"""        def get_domain_name(url):
            # algorithm for extracting domain name from url  with possible subdomians
            # proposed by buckyroberts (https://github.com/buckyroberts) has been 
            # proven to be successful much more frequently then one suggested by me.
            try:
                results = get_sub_domain_name(url).split('.')
                if str(url).startswith('http') == True:
                    return results[-2] + '.' + results[-1]
                else:
                    return 'https://'+results[-2] + '.' + results[-1]
            except:
                return url
        def get_sub_domain_name(url):
            try:
                return urlparse(url).netloc
            except:
                return ''"""


"""            except Exception:
                             request = SeleniumRequest(url=url, callback=self.parse, meta={'splash': {'endpoint': 'render.html', 
                                                                                         'args': {'html': 1,
                                                                                                 'png': 1,
                                                                                                 'width': 600,
                                                                                                 'render_all': 1,
                                                                                                 'wait': 0.5}}})"""             
            #print(get_domain_name(url))
            #exit()


'''            if len(address) > 0:
                if len(address)> 0:
                    url_geo = "http://geocoder.ca/?locate={0}&geoit=xml".format(address)
                    try:
                        r = requests.get(url_geo, timeout=30)
                        root = etree.fromstring(r.text)
                    except Exception:
                        pass
                for child in root:
                    if child.tag == 'error':
                        for item in child:
                            address = ''
                        break
'''
