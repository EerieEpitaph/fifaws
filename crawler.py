import pandas as pd
import re
import os
import requests
from bs4 import BeautifulSoup


class Bailout(Exception):
    pass


# Base program informations (to be argv-ed)
datasets_path = "./datasets/"
maxPlayerOffset = 1  # Default = 350 for 20 000+ players

# Core-data initializing
root_url = "https://sofifa.com"
columns = ['ID', 'Name', 'Age', 'Nationality', 'Overall', 'Potential', 'Club', 'Value', 'Wage', 'TotStats']
data = pd.DataFrame(columns=columns)

# Make datasets target folder
if not os.path.exists(datasets_path):
    os.makedirs(datasets_path)

# Find all dataset versions
source_code = requests.get(root_url)
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
full_body = soup.find('body')
optgroups = full_body.findAll('optgroup')

dataset_dates = []  # Creating datenames and link arrays
dataset_links = []

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
    print("\nGrabbing " + dataset_dates[i] + " (" + str(i+1) + " of " + str(dataset_len) + ")")
    try:
        data = pd.DataFrame(columns=columns)
        currOffset = 1
        for offset in range(0, maxPlayerOffset):
            url = root_url + dataset_links[i] + '&col=tt&sort=desc&offset=' + str(offset * 61) # Order by total value
            # url = root_url + dataset_links[i] + '&offset=' + str(0) # TEST DUPLICATE
            source_code = requests.get(url)
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            table_body = soup.find('tbody')
            rows = table_body.findAll('tr')

            # Lookahead for duplicates
            first_of_batch = rows[0].find('td').find('img').get('id')
            if first_of_batch in data.ID.values:
                print(str(first_of_batch) + " bailed!")
                print('End of offset encountered, bailing out')  # This "if" checks that we do not duplicate any data.
                raise Bailout

            # Populating base infos
            for row in rows:
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

    except Bailout:
        pass
    ######################################
    # Detailed info of player, THANKS! to -insert author, can't remember now-
    detailed_columns = ['Preferred Foot', 'International Reputation', 'Weak Foot', 'Skill Moves', 'Work Rate',
                        'Body Type', 'Position', 'Height', 'Weight', 'LS', 'ST', 'RS', 'LW', 'LF', 'CF', 'RF', 'RW',
                        'LAM', 'CAM', 'RAM', 'LM', 'LCM', 'CM', 'RCM', 'RM', 'LWB', 'LDM', 'CDM', 'RDM', 'RWB', 'LB',
                        'LCB', 'CB', 'RCB', 'RB', 'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing', 'Volleys',
                        'Dribbling', 'Curve', 'FKAccuracy', 'LongPassing', 'BallControl', 'Acceleration', 'SprintSpeed',
                        'Agility', 'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'LongShots',
                        'Aggression', 'Interceptions', 'Positioning', 'Vision', 'Composure', 'Marking',
                        'StandingTackle', 'SlidingTackle', 'GKDiving', 'GKHandling', 'GKKicking', 'GKPositioning',
                        'GKReflexes', 'ID']
    detailed_data = pd.DataFrame(index=range(0, data.count()[0]), columns=detailed_columns)
    detailed_data.ID = data.ID.values
    player_data_url = 'https://sofifa.com/player/'
    for id_final in data.ID:
        url = player_data_url + str(id_final)
        print(url)
        source_code = requests.get(url)
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        body = soup.find('body').find('div', recursive=False).find('div', recursive=False)
        player_card = soup.find("div", attrs={"class": "bp3-card player"})
        aside = soup.find("div", attrs={"class": "bp3-callout spacing calculated"})
        print(player_card)
        print('\n\n')
        print(aside)
        exit(0)

        skill_map = {}


    data = data.drop_duplicates()
    sane_date = re.sub(' ', '_', dataset_dates[i])
    full_data = pd.merge(data, detailed_data, how='inner', on='ID')
    full_data.to_csv(datasets_path + sane_date + '.csv', encoding='utf-8-sig')