import os
import sys
import requests
import threading
from functools import reduce
from collections import Counter
from itertools import combinations
from flask import Flask, jsonify, request, make_response

def create_pairs(data):
    pairs_list = []
    for key, values in data.items():
        # Generate all possible pairs of values for the current key
        pairs = list(combinations(values, 2))
        # print("pracuje")
        # Create dictionaries for each pair and add them to the pairs_list
        for pair in pairs:
            pairs_list.append({pair[0]: pair[1]})
            pairs_list.append({pair[1]: pair[0]})
    return pairs_list

def map_data(raw_data):
    filtered_data = filter(lambda x: len(x.split(';')) >= 3, raw_data)
    # Applying lambda to convert each filtered element to a dictionary
    mapped = list(map(lambda x: {x.split(';')[2]: x.split(';')[0]}, filtered_data))
    for record in mapped:
        shuffle_list[hash(str(record.keys()))%no_workers].append(record)
    mapped_data.append(shuffle_list[int(PORT) - starting_port])
    response = requests.post(master_ready_url, json='ready for shuffling')

def prepare_data_for_shuffle2(users_pairs):
    for record in users_pairs:
        shuffle_list_2[hash(str(record.keys()))%no_workers].append(record)
    final_reduce.append(shuffle_list_2[int(PORT) - starting_port])
    response = requests.post(master_ready_url, json='ready for shuffling 2')

def reduce_data():
    users_pairs = []
    print(f"Got {len(mapped_data)} chunks of data")
    print("Starting reducing")
    flattened_list = reduce(lambda x, y: x + y, mapped_data)
    song_users = reduce(lambda acc, d: {**acc, **{k: acc.get(k, []) + [v] for k, v in d.items()}}, flattened_list, {})
    songs_users_no_duplicates = {song: list(set(users)) for song, users in song_users.items()}
    users_pairs.extend(create_pairs(songs_users_no_duplicates))
    print(f"Reducing finished, {len(users_pairs)} user-user pairs created")
    prepare_data_for_shuffle2(users_pairs)

def reduce_data_2(data):
    print(f"Got {len(data)} chunks of data")
    print("Starting second reducing")
    data = reduce(lambda x, y: x + y, data)
    counts = {}
    for item in data:
        for key, value in item.items():
            counts.setdefault(key, Counter()).update([value])

    # Find the most common username for each user
    results = {}
    for key, counter in counts.items():
        results[key] = counter.most_common(1)[0][0]
    print("Sending final results to Master")
    response = requests.post(master_url, json=results)

app = Flask(__name__)

@app.route('/map', methods = ['POST'])
def get_data():
    data = request.get_json()
    print("received data from master")
    # map_data(data)
    map_thread = threading.Thread(target=map_data, args=(data,))
    map_thread.start()
    return make_response('', 200)

@app.route('/shuffle', methods = ['POST'])
def get_shuffle_comand():
    command = request.get_json()
    if command == 'shuffle':
        print("Starting shuffling")
        for index, worker_url in enumerate(workers_reduce_urls):
            if worker_url != f"http://localhost:{PORT}/reduce":
                print(f"sending data to: {worker_url}")
                response = requests.post(worker_url, json=shuffle_list[index])
    elif command == 'shuffle 2':
        print("Starting second shuffling")
        for index, worker_url in enumerate(workers_reduce2_urls):
            if worker_url != f"http://localhost:{PORT}/reduce2":
                print(f"sending data to: {worker_url}")
                response = requests.post(worker_url, json=shuffle_list_2[index])
    return make_response('', 200)

@app.route('/reduce', methods = ['POST'])
def get_shuffled():
    data = request.get_json()
    print("Received data from another worker")
    mapped_data.append(data)
    if len(mapped_data) == no_workers:
        reduce_thread = threading.Thread(target=reduce_data)
        reduce_thread.start()
    return make_response('', 200)

@app.route('/reduce2', methods = ['POST'])
def get_shuffled2():
    data = request.get_json()
    print("Received data from another worker")
    final_reduce.append(data)
    if len(final_reduce) == no_workers:
        reduce_thread = threading.Thread(target=reduce_data_2, args=(final_reduce,))
        reduce_thread.start()
    return make_response('', 200)

if __name__ == '__main__':
    print('start')
    if len(sys.argv) != 2:
        print("Provide port number")
        sys.exit(1)
    os.environ['PYTHONHASHSEED'] = '123'
    PORT = sys.argv[1]
    no_workers = 4
    mapped_data = []
    shuffle_list = [[]for _ in range(no_workers)]
    shuffle_list_2 = [[]for _ in range(no_workers)]
    final_reduce = []
    starting_port = 5000
    master_port = starting_port + no_workers + 1
    master_url = f'http://localhost:{master_port}/aggregate'
    master_ready_url = f'http://localhost:{master_port}/controller'
    workers_reduce_urls = [f'http://localhost:{port_worker}/reduce' for port_worker in range(starting_port, starting_port + no_workers)]
    workers_reduce2_urls = [f'http://localhost:{port_worker}/reduce2' for port_worker in range(starting_port, starting_port + no_workers)]
    app.run(debug = True, port = PORT)