import requests
from bs4 import BeautifulSoup
import pandas as pd


def base_multi_download(num_of_threads, thread_pid, tot_player_offset, players_per_page, root_url,
                        list_of_dataset_links, body_list, ):
    dataset_len = len(list_of_dataset_links)
    thread_cycles = int(tot_player_offset / num_of_threads)
    for i in range(1):  # was dataset_len IMPORTANT
        for x in range(0, thread_cycles):
            curr_offset = thread_pid * players_per_page + (x * num_of_threads * players_per_page)
            # print(str(thread_pid) + " with offset " + str(curr_offset))
            url = root_url + list_of_dataset_links[i] + '&col=tt&sort=desc&offset=' + str(curr_offset)
            src = requests.get(url)
            txt = src.text
            soop = BeautifulSoup(txt, 'html.parser')
            table_body = soop.find('tbody')
            body_list.append(table_body)
    print("Thread " + str(thread_pid) + " of " + str(num_of_threads) + " finished downloading its share")


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
            print(list_of_frames)
    print("Thread " + str(thread_pid) + " of " + str(num_of_threads) + " finished base parsing")


def adv_multi_downloader(num_of_threads, thread_pid, ):
    print("miao")