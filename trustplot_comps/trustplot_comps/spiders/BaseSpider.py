""" 
    Update from 04-Dec-2019

    Spider developed to scrape gov sites for Pissed Consumer;
    a good deal of functionality should be atributed to
    Vitaliy Bulah. Generally scraping process is doing okay
    although some improvement should be addressed:

    - 403 error and ways to handle it
    - optimization of the mechanism to parse meta tags and properties
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
from shutil import which
from scrapy_selenium import SeleniumRequest

#yield SeleniumRequest(url=url, callback=self.parse_result)

SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('/home/val/coding/chromedriver')
SELENIUM_DRIVER_ARGUMENTS=['-headless']


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
data_base = pd.read_csv('/home/val/coding/scrapy_projects/usa_gov_base.csv', encoding = 'unicode_escape') 

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

    name = 'base'

    def __init__(self):
        super(Spider, self).__init__()
        self.urls = self._load_urls()
        #self.urls = []
        self.java_script = False

    def _load_urls(self):
        #data_base = pd.read_csv('/home/val/coding/scrapy_projects/usa_gov_base.csv') 
        return [link for link in data_base['website']]

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

        def find_phones(response, contact_link):
            # data_base.loc[data_base.website == response.url]
            #print('-----------------')
            soup = BeautifulSoup(response.body, 'lxml')
            for script in soup(["script", "style"]):
                script.extract()
            text = soup.get_text().split('\n')
            phones = []
            i = 0
            for item in text:
                if len(item) <=1:
                    continue
                phone_numbers = re.findall(r"([\+, \(, \), \-, \_, 0-9]{,20})", item)
                phone_numbers_text = re.findall(r"([\+, \(, \), \-, \_, 0-9]{,20}-[a-z, A-Z]{3,5})", item)
                phone_numbers = phone_numbers + phone_numbers_text

                if item.__contains__('@'):
                    phones.append(['@', item])

                if len(phone_numbers) == 0:
                    continue

                selected = []
                for phone in phone_numbers:
                    tmp = phone.replace(' ','').replace('(', '').replace(')', '').replace('-','').replace('_', '')
                    if len(tmp)>=7:
                        selected.append(phone)

                if len(selected) > 0:
                    phones.append(selected + [item])
                i+=1
            for each in phones:
                for i in range(len(each)):
                    each[i] = numerize_string(replace_numbers(each[i]))
            phone_list = ''
            for dim in phones:
                for num in dim:
                    try:
                        if sum(c.isdigit() for c in num) > 6 and sum(c.isdigit() for c in num) < 11:
                            phone_list = phone_list+num+'\n'
                    except Exception:
                        pass
            try:
                assert len(contact_link) > 0
                if len(phone_list) == 0:
                    request = SeleniumRequest(url=contact_link, callback=find_phones, meta={'splash': {'endpoint': 'render.html', 
                                                                                                    'args': {'html': 1,
                                                                                                            'png': 1,
                                                                                                            'width': 600,
                                                                                                            'render_all': 1,
                                                                                                            'wait': 0.5}}})
            except Exception:
                pass

            return phone_list

        def find_address(responce, contact_link):
            soup = BeautifulSoup(response.body, 'lxml')
            for script in soup(["script", "style"]):
                script.extract()
            #text = soup.get_text().split('\n')
            try:
                address = str(pyap.parse(soup.text, country='US')[0])

            except Exception as e:
                print(e)
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
                print(e)
                pass


            return address

        def find_email(responce, contact_link):
            email_pattern = '(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)'

            soup = BeautifulSoup(response.body, 'lxml')
            for script in soup(["script", "style"]):
                script.extract()
            #text = soup.get_text().split('\n')
            for each in soup.get_text().split('\n'):
                try:
                    email_re = re.findall(email_pattern, each)
                    if len(email_re[0]) > 5 and len(email_re[0]) < 75:
                        email = email_re[0]

                except Exception as e:
                    #print(e)
                    pass
                    try:
                        assert len(contact_link) > 0
                        if len(email) == 0:
                            request = SeleniumRequest(url=contact_link, callback=find_email, meta={'splash': {'endpoint': 'render.html', 
                                                                                                    'args': {'html': 1,
                                                                                                            'png': 1,
                                                                                                            'width': 600,
                                                                                                            'render_all': 1,
                                                                                                            'wait': 0.5}}})
                    except Exception as e:
                        email = None

            return email

        def find_state_city(address):
            try:
                if len(address) > 0:
                    #if len(frame['city_2']) > 0 and len(frame['state_2']) > 0:
                       #pass
                    #else:
                    for ct in cities:
                        if ct in frame['address']:
                            city = ct
                            break

                    for st in abbrs.keys():
                        if st in frame['address'] or abbrs[st] in frame['address']:
                            state = st
                            break
                else:
                    city = None
                    state = None

            except Exception:
                return None, None

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


        frame = pd.Series()

        columns=['category', 'name', 'website', 'toll-free', 'email', 'tty', 
                'phone', 'state', 'level', 'name_2', 'complaint', 'state_2',
                'city_2', 'address', 'phones_2', 'email_2', 'name_2']

        ### exctract phones
        print(response.status, '---', response.url)
        if response.status == 200:   
        #soup = BeautifulSoup(response.body, 'lxml')
        #    new_urls = []
        #    for link in soup.find_all('a'):
        #        new_urls.append(link.get('href'))
            #soup = BeautifulSoup(response.headers, 'lxml')
            current_url = response.url
            for url in response.xpath('//a/@href').re(r'.+?complaint.+?'):
                if url.__contains__('http'):
                    complaint_link = url
                else:
                    tmp = current_url.split('/')
                    if url.__contains__(tmp[2]):
                        complaint_link = tmp[0] + url
                    elif current_url[-1] == '/':
                        complaint_link =current_url[:-1] + url
                    else:
                        complaint_link = current_url + url

            for url in response.xpath('//a/@href').re(r'.+?contact.+?'):
                if url.__contains__('http'):
                    contact_link = url
                else:
                    tmp = current_url.split('/')
                    if url.__contains__(tmp[2]):
                        contact_link = tmp[0] + url
                    elif current_url[-1] == '/':
                        contact_link =current_url[:-1] + url
                    else:
                        contact_link = current_url + url
            try:
                assert len(contact_link) > 0
            except Exception:
                contact_link = ''
            try:
                frame['complaint_link'] = complaint_link
            except UnboundLocalError:
                frame['complaint_link'] = ''
            frame['website'] = current_url
            frame['phones_2'] = find_phones(response, contact_link)
            frame['address'] = find_address(response, contact_link)
            frame['email_2'] = find_email(response, contact_link)
            try:
                frame['city_2'], frame['state_2'] = find_state_city(frame['address'])
            except Exception:
                frame['city_2'], frame['state_2'] = None, None
            frame['name_2'] = find_name(response, contact_link)


            #frame['index'] = index


        else:
            frame['website'] = current_url
            frame['phones_2'], frame['address'], frame['email_2'], frame['city_2'], \
            frame['state_2'], frame['name_2'], frame['complaint_link'] = \
            None, None, None, None, None, None, None


        print(frame)
        #print(response.xpath('//meta[@name="description"]/@content').getall()[0])
        #response.xpath("//meta[@name='keywords']/@content")[0].extract()
        data_frame = data_frame.append(frame, ignore_index=True)
        data_frame.to_csv('data_usa_gov_from_sites.csv', header=False, mode='a')
        #index += 1



            #print(phones) 
            #frame phones = [phone+'\n' for phone in phones if phone != None]

            ### extract address/city/state



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
        print(string)
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
