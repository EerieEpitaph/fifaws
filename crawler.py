import pandas as pd
import re
import os
import requests
from bs4 import BeautifulSoup

# Base program informations (to be argv-ed)
datasets_path = "./datasets/"
maxPlayerOffset = 1  # Default = 334 for 20 000+ players
###########################################

# Core-data initializing
root_url = "https://sofifa.com"
columns = ['ID', 'Name', 'Age', 'Nationality', 'Overall', 'Potential', 'Club', 'Value', 'Wage', 'TotStats']
data = pd.DataFrame(columns = columns)
###########################

# Make datasets target folder
if not os.path.exists(datasets_path):
    os.makedirs(datasets_path)
##############################

# Find all dataset versions
source_code = requests.get(root_url)
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
full_body = soup.find('body')
optgroups = full_body.findAll('optgroup')

dataset_dates = []  # Creating datenames and link arrays
dataset_links = []
############################

# Extracting drop-down menu infos
for opt in optgroups:
    dataset_month_year = opt['label']
    if not dataset_month_year.startswith('FIFA'):
        continue

    dataset_month_year = re.sub('^(([A-Z])* ([0-9]+) )', '', dataset_month_year)  # Sanitizing input data
    dataset_month_year = re.sub('  ', ' ', dataset_month_year)

    opt_sublinks = opt.findAll('option')
    for sublink in opt_sublinks:
        dataset_link = sublink['value']
        dataset_day = sublink.text
        dataset_date = dataset_day + " " + dataset_month_year

        sane_date = re.sub(' ', '_', dataset_date)
        if os.path.isfile(datasets_path + sane_date + '.csv'):  # Cancelling download of already-present datasets
            print("Skipping " + dataset_date)
        else:
            dataset_dates.append(dataset_date)
            dataset_links.append(dataset_link)
            print("To download: " + dataset_date)


# Grabbing all 60-uples for any given dataset version
dataset_len = len(dataset_links)
for i in range(dataset_len):
    print("\nGrabbing " + dataset_dates[i] + " (" + str(i) + " of " + str(dataset_len) + ")")

    data = pd.DataFrame(columns=columns)
    currOffset = 1
    for offset in range(0, maxPlayerOffset):
        url = root_url + dataset_links[i] + '&offset=' + str(offset * 61)
        source_code = requests.get(url)
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        table_body = soup.find('tbody')

        # Populating base infos
        for row in table_body.findAll('tr'):
            td = row.findAll('td')
            pid = td[0].find('img').get('id')
            nationality = td[1].find('a').get('title')
            name = td[1].findAll('a')[1].text
            age = td[2].text
            overall = td[3].text.strip()
            potential = td[4].text.strip()
            club = td[5].find('a').text
            value = td[7].text.strip()
            wage = td[8].text.strip()
            totStats = td[10].text.strip()
            player_data = pd.DataFrame([[pid, name, age, nationality, overall, potential, club, value, wage, totStats]])
            player_data.columns = columns
            data = data.append(player_data, ignore_index=True)
        # A che punto siamo?
        print('Batch ' + str(currOffset) + ' of ' + str(maxPlayerOffset) + ' done')
        currOffset = currOffset + 1

    data = data.drop_duplicates()
    #print(data)
    sane_date = re.sub(' ', '_', dataset_dates[i])
    data.to_csv(datasets_path + sane_date + '.csv', encoding='utf-8-sig', index=False)

'''
# Informazioni dettagliate sul singolo giocatore
detailed_columns = ['Preferred Foot', 'International Reputation', 'Weak Foot', 'Skill Moves', 'Work Rate', 'Body Type', 'Real Face', 'Position', 'Jersey Number', 'Joined', 'Loaned From', 'Contract Valid Until', 'Height', 'Weight', 'LS', 'ST', 'RS', 'LW', 'LF', 'CF', 'RF', 'RW', 'LAM', 'CAM', 'RAM', 'LM', 'LCM', 'CM', 'RCM', 'RM', 'LWB', 'LDM', 'CDM', 'RDM', 'RWB', 'LB', 'LCB', 'CB', 'RCB', 'RB', 'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing', 'Volleys', 'Dribbling', 'Curve', 'FKAccuracy', 'LongPassing', 'BallControl', 'Acceleration', 'SprintSpeed', 'Agility', 'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'LongShots', 'Aggression', 'Interceptions', 'Positioning', 'Vision', 'Penalties', 'Composure', 'Marking', 'StandingTackle', 'SlidingTackle', 'GKDiving', 'GKHandling', 'GKKicking', 'GKPositioning', 'GKReflexes', 'ID']
detailed_data = pd.DataFrame(index = range(0, data.count()[0]), columns = detailed_columns)
detailed_data.ID = data.ID.values

player_data_url = 'https://sofifa.com/player/'
for id in data.ID:
    url = player_data_url + str(id)
    print(url)
    source_code = requests.get(url)
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text, 'html.parser')
    skill_map = {}
    columns = soup.find('div', {'class': 'teams'}).find('div', {'class': 'columns'}).findAll('div', {'class': 'column col-4'})
    for column in columns:
        skills = column.findAll('li')
        for skill in skills:
            if(skill.find('label') != None):
                label = skill.find('label').text
                value = skill.text.replace(label, '').strip()
                skill_map[label] = value
    meta_data = soup.find('div', {'class': 'meta'}).text.split(' ')
    length = len(meta_data)
    weight = meta_data[length - 1]
    height = meta_data[length - 2].split('\'')[0] + '\'' + meta_data[length - 2].split('\'')[1].split('\"')[0]
    skill_map["Height"] = height
    skill_map['Weight'] = weight
    if('Position' in skill_map.keys()):
        if skill_map['Position'] in ('', 'RES', 'SUB'):
            skill_map['Position'] = soup.find('article').find('div', {'class': 'meta'}).find('span').text
        if(skill_map['Position'] != 'GK'):
            card_rows = soup.find('aside').find('div', {'class': 'card mb-2'}).find('div', {'class': 'card-body'}).findAll('div', {'class': 'columns'})
            for c_row in card_rows:
                attributes = c_row.findAll('div', {'class': re.compile('column col-sm-2 text-center')})
                for attribute in attributes:
                    if(attribute.find('div')):
                        name = ''.join(re.findall('[a-zA-Z]', attribute.text))
                        value = attribute.text.replace(name, '').strip()
                        skill_map[str(name)] = value
    sections = soup.find('article').findAll('div', {'class': 'mb-2'})[1:3]
    print(sections)
    first = sections[0].findAll('div', {'class': 'column col-4'})
    second = sections[1].findAll('div', {'class': 'column col-4'})[:-1]
    sections = first + second
    for section in sections:
        items = section.find('ul').findAll('li')
        for item in items:
            value = int(re.findall(r'\d+', item.text)[0])
            name = ''.join(re.findall('[a-zA-Z]*', item.text))
            skill_map[str(name)] = value
    for key, value in skill_map.items():
        detailed_data.loc[detailed_data.ID == id, key] = value

full_data = pd.merge(data, detailed_data, how = 'inner', on = 'ID')
full_data.to_csv('data.csv', encoding='utf-8-sig')
'''
