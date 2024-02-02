import sys
from flask import Flask, jsonify, request, make_response
import requests
import time
from functools import reduce
from itertools import combinations
import threading


def create_pairs(data):
    # Initialize an empty list to store the pairs
    pairs_list = []

    # Iterate over the dictionary
    for key, values in data.items():
        # Generate all possible pairs of values for the current key
        pairs = list(combinations(values, 2))
        # Create dictionaries for each pair and add them to the pairs_list
        for pair in pairs:
            pairs_list.append({pair[0]: pair[1]})
            pairs_list.append({pair[1]: pair[0]})
    return pairs_list

def map_data(data):
    filtered_data = filter(lambda x: len(x.split(';')) >= 3, data)

    # Applying lambda to convert each filtered element to a dictionary
    mapped = list(map(lambda x: {x.split(';')[2]: x.split(';')[0]}, filtered_data))
    for record in mapped:
        shuffle_list[hash(str(record.keys()))%no_workers].append(record)
    mapped_data.append(shuffle_list[int(PORT) - starting_port])

def reduce_data():
    print("Received reduce command")
    print("Starting reducing")
    print(f"Got {len(mapped_data)} chunks of data")
    flattened_list = reduce(lambda x, y: x + y, mapped_data)
    song_users = reduce(lambda acc, d: {**acc, **{k: acc.get(k, []) + [v] for k, v in d.items()}}, flattened_list, {})
    final_pairs.extend(create_pairs(song_users))
    print(f"Reducing finished, {len(final_pairs)} pairs created")
    # time.sleep(10)
    # response = requests.post(master_url, json=pairs)

app = Flask(__name__)

@app.route('/map', methods = ['POST'])
def get_data():
    data = request.get_json()
    print("received data from master")
    # Filter out records with insufficient elements
    map_data(data)

    # for index, worker_url in enumerate(workers_urls):
    #     if worker_url != f"http://localhost:{PORT}/reduce":
    #         print(f"sending to: {worker_url}")
    #         response = requests.post(worker_url, json=shuffle_list[index])
    
    # if len(mapped_data) == no_workers:
    #     reduce_data()
    #     reduce_thread = threading.Thread(target=reduce_data)
    #     reduce_thread.start()
    return make_response('', 200)

@app.route('/shuffle', methods = ['POST'])
def get_shuffle_comand():
    data = request.get_json()
    if data == 'shuffle':
        # time.sleep(10)
        for index, worker_url in enumerate(workers_urls):
            if worker_url != f"http://localhost:{PORT}/reduce":
                print(f"sending to: {worker_url}")
                response = requests.post(worker_url, json=shuffle_list[index])
    return make_response('', 200)

@app.route('/reduce', methods = ['POST'])
def get_shuffled():
    data = request.get_json()
    if data == "reduce":
        reduce_thread = threading.Thread(target=reduce_data)
        reduce_thread.start()
    else:
        print("Received data from another worker")
        mapped_data.append(data)
        # if len(mapped_data) == no_workers:
        #     reduce_thread = threading.Thread(target=reduce_data)
        #     reduce_thread.start()
    return make_response('', 200)

@app.route('/aggregate', methods = ['POST'])
def get_aggregate_comand():
    data = request.get_json()
    if data == 'aggregate':
        print(f"Sending {len(final_pairs)} pairs to master")
        response = requests.post(master_url, json=final_pairs)
    return make_response('', 200)


if __name__ == '__main__':
    print('start')
    if len(sys.argv) != 2:
        print("Provide port number")
        sys.exit(1)
    
    PORT = sys.argv[1]
    mapped_data = []
    shuffle_list = [[],[],[],[]]
    final_pairs = []
    starting_port = 5000
    no_workers = 4
    master_port = starting_port + no_workers + 1
    master_url = f'http://localhost:{master_port}/aggregate'
    workers_urls = [f'http://localhost:{port_worker}/reduce' for port_worker in range(starting_port, starting_port + no_workers)]
    app.run(debug = True, port = PORT)