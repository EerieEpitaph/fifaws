import numpy as np
import pandas as pd
import re
import os
import requests
import threading
from bs4 import BeautifulSoup


class Bailout(Exception):
    pass


# Base program information (to be argv-ed)
datasets_path = "./datasets/"
maxPlayerOffset = 360  # Default = 360 for 20 000+ players
off_mult = 61  # Default = 61
one_in = 10  # Download one dataset every X found
thread_count = 60
base_bodies = []
adv_parse = []

# Core-data initializing
root_url = "https://sofifa.com"
columns = ['ID', 'Name', 'Age', 'Nationality', 'Overall', 'Potential', 'Club', 'Value', 'Wage', 'TotStats']
base_data = pd.DataFrame(columns=columns)

# Make datasets target folder
if not os.path.exists(datasets_path):
    os.makedirs(datasets_path)

# Find all dataset versions
source_code = requests.get(root_url)
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
full_body = soup.find('body')
optgroups = full_body.findAll('optgroup')

dataset_dates = []  # Creating dates and link arrays
dataset_links = []

# Extracting drop-down menu infos
print("Downloading one dataset every " + str(one_in) + " found")
found_counter = 0
for opt in optgroups:
    dataset_month_year = opt['label']
    if not dataset_month_year.startswith('FIFA'):
        continue

    dataset_month_year = re.sub('^(([A-Z])* ([0-9]+) )', '', dataset_month_year)  # Sanitizing input data
    dataset_month_year = re.sub('  ', ' ', dataset_month_year)

    opt_sublinks = opt.findAll('option')
    for sublink in opt_sublinks:
        found_counter = found_counter + 1
        dataset_link = sublink['value']
        dataset_day = sublink.text
        dataset_date = dataset_day + " " + dataset_month_year

        sane_date = re.sub(' ', '_', dataset_date)
        if found_counter % one_in != 0:
            print("Skipping " + dataset_date + " due to options")
            continue
        elif os.path.isfile(datasets_path + sane_date + '.csv'):  # Cancelling download of already-present datasets
            print("Skipping " + dataset_date + " due to copy present")
        else:
            dataset_dates.append(dataset_date)
            dataset_links.append(dataset_link)
            print("To download: " + dataset_date)


def base_multi_download(num_of_threads, thread_pid, list_of_dataset_links, body_list,):
    dataset_len = len(dataset_links)
    thread_cycles = int(maxPlayerOffset / thread_count)
    for i in range(1):  # was dataset_len IMPORTANT
        for x in range(0, thread_cycles):
            curr_offset = thread_pid * off_mult + (x * num_of_threads * off_mult)
            # print(str(thread_pid) + " with offset " + str(curr_offset))
            url = root_url + list_of_dataset_links[i] + '&col=tt&sort=desc&offset=' + str(curr_offset)
            src = requests.get(url)
            txt = src.text
            soop = BeautifulSoup(txt, 'html.parser')
            table_body = soop.find('tbody')
            body_list.append(table_body)
    # print("Thread " + str(thread_pid) + " of " + str(num_of_threads) + " finished downloading its share")


def base_multi_parser(num_of_threads, thread_pid, my_body_slice, list_of_frames):
    for body in my_body_slice:
        rows = body.findAll('tr')

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
            tot_stats = td[10].text.strip()
            player_data = pd.DataFrame(
                [[pid, name, age, nationality, overall, potential, club, value, wage, tot_stats]])
            player_data.columns = columns
            list_of_frames.append(player_data)
    # print("Thread " + str(thread_pid) + " of " + str(num_of_threads) + " finished base parsing")


def adv_multi_downloader(num_of_threads, thread_pid, ):
    print("miao")

frame_list = []
thread_list = []
print("Activating " + str(thread_count) + " threads")
for x in range(0, thread_count):
    thread = threading.Thread(target=base_multi_download, args=(thread_count, x, dataset_links, base_bodies,))
    thread_list.append(thread)
    thread.start()
for thread in thread_list:
    thread.join()
print("Base info downloaded")

npized_bodies = np.array(base_bodies)
chunks_of_bodies = np.split(npized_bodies, thread_count)

thread_list = []
for x in range(0, thread_count):
    thread = threading.Thread(target=base_multi_parser, args=(thread_count, x, chunks_of_bodies[x], frame_list, ))
    thread_list.append(thread)
    thread.start()
for thread in thread_list:
    thread.join()

print("Stitching base data together")
for frame in frame_list:
    base_data = base_data.append(frame, ignore_index=True)
base_data = base_data.drop_duplicates()
base_data = base_data.sort_values(by=['TotStats'])

print(base_data)

print("Downloading detailed player data")
ID_list = base_data.ID.values
print(ID_list)
thread_list = []
for x in range(0, thread_count):
    thread = threading.Thread(target=adv_multi_downloader(), args=(thread_count, x, ))
    thread_list.append(thread)
    thread.start()
for thread in thread_list:
    thread.join()


'''
# Grabbing all 60-uples for any given dataset version
dataset_len = len(dataset_links)

for i in range(dataset_len):
    print("\nGrabbing " + dataset_dates[i] + " (" + str(i+1) + " of " + str(dataset_len) + ")")
    try:
        data = pd.DataFrame(columns=columns)
        currOffset = 1
        for offset in range(0, maxPlayerOffset):
            url = root_url + dataset_links[i] + '&col=tt&sort=desc&offset=' + str(offset * off_mult)  # Order by ttvalue
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
            print('Basic info batch ' + str(currOffset) + ' of ' + str(maxPlayerOffset) + ' done')
            currOffset = currOffset + 1

    except Bailout:
        pass

    # Detailed info of player, THANKS! to -insert author, can't remember now-
    curr_player = 1
    players_num = len(data.ID.values)
    detailed_columns = ['ID', 'PreferredFoot', 'InternationalReputation', 'WeakFoot', 'WorkRate',
                        'BodyType', 'Position', 'Height', 'Weight', 'LS', 'ST', 'RS', 'LW', 'LF', 'CF', 'RF', 'RW',
                        'LAM', 'CAM', 'RAM', 'LM', 'LCM', 'CM', 'RCM', 'RM', 'LWB', 'LDM', 'CDM', 'RDM', 'RWB', 'LB',
                        'LCB', 'CB', 'RCB', 'RB', 'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing', 'Volleys',
                        'Dribbling', 'Curve', 'FKAccuracy', 'LongPassing', 'BallControl', 'Acceleration', 'SprintSpeed',
                        'Agility', 'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'LongShots',
                        'Aggression', 'Interceptions', 'Positioning', 'Vision', 'Penalties', 'Composure', 'Marking',
                        'StandingTackle', 'SlidingTackle', 'GKDiving', 'GKHandling', 'GKKicking', 'GKPositioning',
                        'GKReflexes']
    detailed_data = pd.DataFrame(columns=detailed_columns)
    player_data_url = 'https://sofifa.com/player/'
    value_names = []
    values = []
    for id_final in data.ID:
        url = player_data_url + str(id_final)
        # print(url)
        source_code = requests.get(url)
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        body = soup.find('body').find('div', recursive=False).find('div', recursive=False)
        player_card = soup.find("div", attrs={"class": "bp3-card player"})
        aside = soup.find("div", attrs={"class": "bp3-callout spacing calculated"})
        top_left_card = player_card.find('ul').findAll('li')

        preferred_foot = top_left_card[0].label.next_sibling
        international_reputation = top_left_card[1].label.next_sibling[0]
        weak_foot = top_left_card[2].label.next_sibling[0]
        work_rate = top_left_card[4].label.next_sibling.text.replace(' ', '')
        body_type = top_left_card[5].label.next_sibling.text
        value_names.append('PreferredFoot')
        value_names.append('InternationalReputation')
        value_names.append('WeakFoot')
        value_names.append('WorkRate')
        value_names.append('BodyType')
        values.append(preferred_foot)
        values.append(international_reputation)
        values.append(weak_foot)
        values.append(work_rate)
        values.append(body_type)

        info = player_card.find('div', attrs={"class": "meta bp3-text-overflow-ellipsis"})
        height_weight_literal = info.text[info.text.rfind(')')+2:]
        height_literal = height_weight_literal[:height_weight_literal.find('"')]
        height_feet = height_literal[0]
        height_inch = height_literal[2:]
        weight_lbs = height_weight_literal[height_weight_literal.find('lbs')-3:height_weight_literal.find('lbs')]

        position = info.span.text
        height = float(height_feet)*30.48 + float(height_inch)*2.54
        weight = float(weight_lbs)*0.453592
        weight = '%.2f' % weight
        value_names.append('Position')
        value_names.append('Height')
        value_names.append('Weight')
        values.append(position)
        values.append(height)
        values.append(weight)

        position_map = aside.find_all('div', attrs={'class': re.compile('column col-sm-2 text-center p[0-9]*')})
        for position in position_map:
            value_names.append(position.div.text)
            values.append(position.div.next_sibling)

        card_columns = player_card.findAll('div', attrs={"class": "column col-4"})
        for column in card_columns:
            try:
                labels = column.ul.findAll('li')
            except AttributeError:
                pass
            for label in labels:
                stat_tuple = label.text.split(' ', 1)
                try:
                    int(stat_tuple[0])
                except ValueError:
                    continue
                value_names.append(str(stat_tuple[-1]).replace(' ', '').replace('\n', ''))
                values.append(stat_tuple[0])

        player_detailed_data = pd.DataFrame(columns=detailed_columns)
        for x in range(0, len(value_names)):
            player_detailed_data[value_names[x]] = [values[x]]
        player_detailed_data['ID'] = id_final
        detailed_data = detailed_data.append(player_detailed_data)
        print("Player " + str(curr_player) + " of " + str(players_num) + " done")
        curr_player = curr_player + 1

    sane_date = re.sub(' ', '_', dataset_dates[i])
    data.ID = data.ID.astype(str)
    detailed_data.ID = detailed_data.ID.astype(str)

    full_data = pd.merge(data, detailed_data, on='ID')
    full_data.to_csv(datasets_path + sane_date + '.csv', encoding='utf-8-sig', index=False)
'''