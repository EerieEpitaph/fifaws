import requests
from bs4 import BeautifulSoup
import pandas as pd


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

def adv_multi_downloader(num_of_threads, thread_pid, ):
    print("miao")


def drain_multi_queue(queue):
    drained_queue = []
    queue_len = queue.qsize()
    curr = 0
    while not queue.empty():
        curr_element = queue.get()
        drained_queue.append(curr_element)
        print(str(curr) + " of " + str(queue_len))
        curr = curr + 1
    return drained_queue

