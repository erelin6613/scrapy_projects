from scrapy import Spider
from scrapy import Request
import json
from .. import settings
from w3lib.http import basic_auth_header
import pandas as pd
from scrapy.selector import Selector
from bs4 import BeautifulSoup
import re

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

frame = pd.Series()


# response.selector.xpath('//article').get()


#frame = pd.read_csv('/home/val/coding/international_companies/trustpilot_data.csv')

abbrs = { 'Alaska': 'AK', 'Alabama': 'AL', 'Arkansas': 'AR', 'Arizona': 'AZ', 'California': 'CA', 
            'Colorado': 'CO', 'Connecticut': 'CT', 'District of Columbia': 'DC', 'Delaware': 'DE', 
            'Florida': 'FL', 'Georgia': 'GA', 'Iowa': 'IA', 'Idaho':    'ID', 
            'Illinois': 'IL', 'Indiana': 'IN', 'Kansas': 'KS', 'Kentucky': 'KY', 
            'Louisiana': 'LA', 'Massachusetts': 'MA', 'Maryland':   'MD', 'Maine': 'ME', 
            'Michigan': 'MI', 'Minnesota': 'MN', 'Missouri': 'MO', 'Mississippi': 'MS',
            'Montana': 'MT', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Nebraska': 'NE', 
            'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'Nevada': 'NV', 
            'New York': 'NY', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR',
            'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 
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
            'Cedar Rapids', 'Sterling Heights', 'New Haven', 'Stamford', 'Concord', 'Kent', 'Santa Clara', 
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
        urls = []
        for state in abbrs.keys():
            urls.append('https://www.usa.gov/state-consumer/{}'.format(state.lower()))
        return urls

        #file_path = '/home/val/coding/international_companies/trustpilot_data.csv'
        #data = pd.read_csv(file_path) #, sep=';', index_col=None)
        #urls = [link for link in data['url']]
        #i = 0
        #for u in list(data['url']):
        #    if u == '' or str(u).lower() == 'nan':
        #        continue
        #    i += 1
        #    urls.append(u)
        #    if i >= 25:
        #        break
        #return urls

    def start_requests(self):
        #return
        #credentials = json.load(open(settings.ProxyPath))
        for url in self.urls:
            request = Request(url=url, callback=self.parse, meta={'splash': {'endpoint': 'render.html', 
                                                                            'args': {'html': 1,
                                                                                    'png': 1,
                                                                                    'width': 600,
                                                                                    'render_all': 1,
                                                                                    'wait': 0.5}}})
            yield request

    def parse(self, response):

        ### exctract phones
        print(response.status, '---', response.url)
        if response.status == 200:
            print('-----------------')
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
            #print(phones) frame phones = [phone+'\n' for phone in phones if phone != None]


            ### extract category, name
            #soup = BeautifulSoup(response.)
            soup = BeautifulSoup(response.body, 'lxml')
            article = soup.find('article')
            #print(article)
            #header = article.find('header')
            data_frame = pd.DataFrame()
            levels = ['state', 'city', 'federal']
            while article is not None:
                try:
                    for each in article.find_all_next('h2'):
                        frame['category'] = each.get_text()
                        frame['name'] = each.find_next('header').find_next('h3').get_text()
                        for field in each.find_all_next('h3'):
                            try:
                                if field.get_text().endswith(':') == True:
                                    if field.get_text().lower() == 'website' or field.get_text().lower() == 'email':
                                        frame['email'] = field.find_next('a').get('href').strip()
                                        frame['website'] = field.find_next('a').get('href').strip().split('@')[1]
                                        #if '@' in field.find_next('a').get('href'):
                                            #frame['website'] = field.find_next('a').get('href').strip().split('@')[1]

                                    else:
                                        frame[field.get_text().strip().lower().strip(':')] = field.find_next('p').get_text().strip()

                            except Exception as e:
                                print(e)
                                pass

                            frame['state'] = response.url.split('/')[-1]
                            for city in cities:
                                if city in frame['name']:
                                    frame['city'] = city.lower()
                            for level in levels:
                                if level in frame['name']:
                                    frame['level'] = level
                        print(frame)
                        check_and_write(frame)
                        #data_frame = data_frame.append(frame, ignore_index=True)

                        #print(responce.text)
                        #for field in each.find_next('h3').descendants:
                            #print(field.get_text())
                            #if field.name == 'h3':
                            #    frame[field.get_text()] = field.get_text()


                except Exception:
                    pass
                article = article.find_next('h2')
            #data_frame.to_csv('./trustpilot_data_gov_usa.csv')
            #print(frame)


            #print(text)

    def check_and_write(frame):
        columns=['category', 'name', 'website', 'toll free', 'email', 'tty', 'phone', 'state', 'level']
        for col in columns:
            try:
                assert len(frame[col]) > 0
            except:
                frame[col] = None
        frame.to_csv('./trustpilot_data_gov_usa.csv', header=False, mode='a')


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
        if key in string:
            string.replace(key, numbers[key])

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

            #print(text)
'''


"""
            header = soup.find("article").find('header')

            while header is not None:
                #print(header.prettify)
                print(header.find('h2').get_text())
                header = header.find_next_sibling()
"""



                try:
                    frame['name'] = header.find_next('h3')[0]
                except Exception:
                    continue
                for elem in article.get_text().split('\n'):
                    if len(elem) > 3 and len(elem) < 40:
                        if elem.endswith(':'):
                            if elem.lower() == 'website:':
                                frame[elem.lower()] = header.find('a').get('href')
                                pass
                            else:
                                frame[elem.lower()] = header.find_next_sibling().text
                                #get_text()
                        else:
                            pass
                            frame['category'] = elem

'''