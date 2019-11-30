import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

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
#frame = pd.Series()
data_frame = pd.DataFrame()
cols = ['name', 'website', 'email', 'phone', 'toll-free', 'tty', 'state', 'city', 'level']
for each in abbrs.keys():
	id_list = []
	r = requests.get('https://www.usa.gov/state-consumer/{}'.format(each.lower()))
	soup = BeautifulSoup(r.text, 'lxml')
	for script in soup(["script", "style"]):
		script.extract()
	ids = soup.find_all(class_='clearfix')[1]
	for link in ids.find_all('a'):
		#print(link)
		if link.get('href').startswith('#'):
			id_list.append(link.get('href'))
		#print(id_list)
		for id_ in id_list:
			frame = pd.Series()
			#data_frame = pd.DataFrame()
			for name in soup.select(id_):
				i = 0
				for tag in name.find_all_next()[1:]:
					if tag.name == 'h2':
						break
					#name = 
					if tag.name == 'section':
						if i == 0:
							frame['name'] = tag.get_text().strip()
							tag.get_text().strip()
						try:
							if tag.find_next('h3').get_text().strip().endswith(':') == True:
								if 'website' in tag.find_next('h3').get_text().strip().lower():
									frame['website'] = tag.find_next('h3').find_next('a').get('href')
								if 'email' in tag.find_next('h3').get_text().strip().lower():
									frame['email'] = tag.find_next('h3').find_next('a').get('href').strip('mailto:')
								if 'phone' in tag.find_next('h3').get_text().strip().lower():
									frame['phone'] = tag.find_next('h3').find_next('p').get_text().strip()
								if 'toll free' in tag.find_next('h3').get_text().strip().lower():
									frame['toll-free'] = tag.find_next('h3').find_next('p').get_text().strip()	
								if 'tty' in tag.find_next('h3').get_text().strip().lower():
									frame['tty'] = tag.find_next('h3').find_next('p').get_text().strip()				
								#frame[tag.find_next('h3').get_text().strip(':')] == tag.find_next('h3').get_text()
							#frame[tag.get_text().strip().split(':')[0]] = tag.get_text().strip().split(':')[1]
						except:
							pass
						i += 1
						levels = ['state', 'city', 'federal']
						frame['state'] = r.url.split('/')[-1]
						for city in cities:
							if city in frame['name']:
								frame['city'] = city.lower()
						for level in levels:
							if level in frame['name']:
								frame['level'] = level
						if len(frame) > 0:
							for col in cols:
								try:
									assert len(frame[col]) > 0
								except:
									frame[col] = None

						#time.sleep(10)
						#print()
						#print(tag.get_text().strip())
							#print(tag.get_text().strip(), '-', tag.find_next('section'))
							#frame[tag.get_text().strip()] = tag.find_next('section').get_text()
			print(frame)
			#data_frame = pd.DataFrame(frame)
			data_frame = data_frame.append(frame, ignore_index=True)
data_frame.to_csv('trustplot_data_usa_gov.csv', header=False, mode='a')
				#for key in frame.keys():
				#	frame[key] = frame[key].strip()
				#print(frame)
						#print(tag.get_text().strip())
