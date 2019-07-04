import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


def base_multi_download(num_of_threads, thread_pid, max_player_pages, players_in_page, root_url,
                        list_of_dataset_links, body_queue, child_conn):
    dataset_len = len(list_of_dataset_links)
    thread_cycles = int(max_player_pages / num_of_threads)
    for i in range(1):  # was dataset_len IMPORTANT
        for x in range(0, thread_cycles):
            curr_offset = thread_pid * players_in_page + (x * num_of_threads * players_in_page)
            # print(str(thread_pid) + " with offset " + str(curr_offset))
            url = root_url + list_of_dataset_links[i] + '&col=tt&sort=desc&offset=' + str(curr_offset)
            src = requests.get(url)
            txt = src.text
            soop = BeautifulSoup(txt, 'html.parser')
            table_body = soop.find('tbody')
            body_queue.put(str(table_body))
    print("Thread " + str(thread_pid) + " of " + str(num_of_threads) + " finished downloading its share")
    child_conn.send(0)


def base_multi_parser(num_of_threads, thread_pid, my_body_slice,
                      base_columns, frame_queue, child_conn):
    for body in my_body_slice:
        soop = BeautifulSoup(body, 'html.parser')
        rows = soop.findAll('tr')

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
            contract = td[5].div.br.text
            value = td[7].text.strip()
            wage = td[8].text.strip()
            tot_stats = td[10].text.strip()
            player_data = pd.DataFrame(
                [[pid, name, age, nationality, overall, potential, club, contract, value, wage, tot_stats]])
            player_data.columns = base_columns
            frame_queue.put(player_data)
    print("Thread " + str(thread_pid) + " of " + str(num_of_threads) + " finished base parsing")
    child_conn.send(0)


def adv_multi_downloader(thread_pid, my_id_slice,
                         player_queue, child_conn):
    player_data_url = 'https://sofifa.com/player/'
    count = 0
    for id in my_id_slice:
        url = player_data_url + str(id)
        # print(url)
        source_code = requests.get(url)
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        body = soup.find('body').find('div', recursive=False).find('div', recursive=False)
        player_queue.put(str(body))

        count = count + 1
        print("Downloaded " + str(count) + " out of " + str(len(my_id_slice)) + " of process " + str(thread_pid))
    child_conn.send(0)


def adv_multi_parser(thread_pid, my_player_slice, parsed_player_queue,
                     detailed_columns, child_conn):
    value_names = []
    values = []
    count = 0
    for player_body in my_player_slice:
        soop = BeautifulSoup(player_body, 'html.parser')
        player_card = soop.find("div", attrs={"class": "bp3-card player"})
        aside = soop.find("div", attrs={"class": "bp3-callout spacing calculated"})
        top_left_card = player_card.find('ul').findAll('li')

        final_id_str = player_card.find("div", attrs={"class": "info"}).h1.strip()
        final_id = final_id_str[final_id_str.rfind(" "):len(final_id_str)]
        print(final_id)

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
        height_weight_literal = info.text[info.text.rfind(')') + 2:]
        height_literal = height_weight_literal[:height_weight_literal.find('"')]
        height_feet = height_literal[0]
        height_inch = height_literal[2:]
        weight_lbs = height_weight_literal[height_weight_literal.find('lbs') - 3:height_weight_literal.find('lbs')]

        position = info.span.text
        height = float(height_feet) * 30.48 + float(height_inch) * 2.54
        weight = float(weight_lbs) * 0.453592
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
        player_detailed_data['ID'] = final_id
        parsed_player_queue.put(player_detailed_data)
        count = count + 1
        print("Player " + str(count) + " of " + str(len(my_player_slice)) + " of thread " + str(thread_pid) + " done")
    child_conn.send(0)


def drain_multi_queue(queue):
    drained_queue = []
    curr = 0
    while not queue.empty():
        curr_element = queue.get()
        drained_queue.append(curr_element)
        curr = curr + 1
    return drained_queue


def chunker_list(seq, size):
    ops = (seq[i::size] for i in range(size))
    return list(ops)