import requests
import csv
from itertools import islice
import requests
from flask import Flask, jsonify, request, make_response
import threading
import sys
import time
from collections import Counter

def split_data(data, no_chunks = 4):
    size = len(data)
    reminder = size%no_chunks
    chunk_size = size // no_chunks
    chunks = []
    lower_bound = 0
    for i in range(no_chunks):
        if 0 < reminder:
            upper_bound = lower_bound + chunk_size + 1
            reminder -= 1
        else: 
            upper_bound = lower_bound + chunk_size
        chunks.append(data[lower_bound:upper_bound])
        lower_bound = upper_bound
    return chunks

def read_csv_to_list_of_dicts(filename):
    data_list = []
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        next(reader)  # Skip the header row
        for row in reader:
            print(row)
            user_id, artistname, trackname, playlistname = row
            data_list.append({
                'user_id': user_id,
                'artistname': artistname,
                'trackname': trackname,
                'playlistname': playlistname
            })
    return data_list

def csv_to_dict(csv_file):
    data = []
    with open(csv_file, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter = ',', quitechar = '"')
        for row in csvreader:
            data.append(row)
    return data

def csv_to_list(filename):
    with open(filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip('"\n'))
        return data

def split_dict_into_parts(data):
    n = len(data)
    part_size = n // no_workers
    parts = [data[i * part_size:(i + 1) * part_size] for i in range(no_workers)]
    return parts

def final_results(results):
    # counts = {}
    # for item in data:
    #     for key, value in item.items():
    #         counts.setdefault(key, Counter()).update([value])

    # # Find the most common hashed username for each key
    # results = {}
    # for key, counter in counts.items():
    #     results[key] = counter.most_common(1)[0][0]
    for result in results:
        for user1, user2 in result.items():
            print(f"User '{user1}' should follow user '{user2}'")

app = Flask(__name__)

@app.route('/aggregate', methods = ['POST'])
def aggregate_data():
    global ag_counter
    data = request.get_json()
    print("received data from worker")
    aggregated_data.append(data)
    print(len(aggregated_data))
    ag_counter += 1
    if ag_counter == no_workers:
        final_results(aggregated_data)
    return make_response('', 200)

@app.route('/controller', methods = ['POST'])
def workflow_control():
    data = request.get_json()
    print(data)
    ready_counter[data] += 1
    if ready_counter['ready for shuffling'] == no_workers:
        ready_counter['ready for shuffling'] = 0
        send_comand("shuffle", workers_shuffle_urls)
    elif ready_counter['ready for reducing'] == no_workers:
        ready_counter['ready for reducing'] = 0
        send_comand("reduce", workers_reduce_urls)
    elif ready_counter['ready for shuffling 2'] == no_workers:
        ready_counter['ready for shuffling 2'] = 0
        send_comand("shuffle 2", workers_shuffle_urls)
    elif ready_counter['ready for reducing 2'] == no_workers:
        ready_counter['ready for reducing 2'] = 0
    return make_response('', 200)

def run_flask_app(port):
    app.run(debug=False, port=port)

def send_data():
    for index, worker_url in enumerate(workers_map_urls):
        print(f"Sending raw data to {worker_url}")
        response = requests.post(worker_url, json=chunks[index])

def send_comand(comand, urls):
    for index, worker_url in enumerate(urls):
        print(f"Sending {comand} comand to {worker_url}")
        response = requests.post(worker_url, json=comand)

def start():
    send_data()
    # send_comand("shuffle", workers_shuffle_urls)
    # send_comand("reduce", workers_reduce_urls)
    # time.sleep(10)
    # send_comand("aggregate", workers_aggregate_urls)

if __name__ == '__main__':
    starting_port = 5000
    no_workers = 4
    workers_map_urls = [f'http://localhost:{port}/map' for port in range(starting_port, starting_port + no_workers)]
    workers_shuffle_urls = [f'http://localhost:{port}/shuffle' for port in range(starting_port, starting_port + no_workers)]
    workers_reduce_urls = [f'http://localhost:{port}/reduce' for port in range(starting_port, starting_port + no_workers)]
    workers_aggregate_urls = [f'http://localhost:{port}/aggregate' for port in range(starting_port, starting_port + no_workers)]
    master_port = starting_port + no_workers + 1
    aggregated_data = []
    ag_counter = 0
    ready_counter = {'ready for shuffling': 0, 'ready for reducing': 0, 'ready for shuffling 2': 0, 'ready for reducing 2': 0}
    # csv_filename = 'data/spotify_dataset.csv'
    csv_filename = 'data/test.csv' 
    csv_data = csv_to_list(csv_filename)
    chunks = split_data(csv_data, no_workers)

    main_thread = threading.Thread(target=start)
    main_thread.start()

    run_flask_app(master_port)
