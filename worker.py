import sys
from flask import Flask, jsonify, request, make_response
import requests
import time
from functools import reduce
from itertools import combinations


def create_pairs(data):
    # Initialize an empty list to store the pairs
    pairs_list = []

    # Iterate over the dictionary
    for key, values in data.items():
        # Generate all possible pairs of values for the current key
        pairs = list(combinations(values, 2))
        # Create dictionaries for each pair and add them to the pairs_list
        print("next")
        for pair in pairs:
            pairs_list.append({pair[0]: pair[1]})
            pairs_list.append({pair[1]: pair[0]})
    return pairs_list

def reduce_data():
    print("Received data from all workers")
    print("Starting reducing")
    flattened_list = reduce(lambda x, y: x + y, mapped_data)
    song_users = reduce(lambda acc, d: {**acc, **{k: acc.get(k, []) + [v] for k, v in d.items()}}, flattened_list, {})
    # print(flattened_list)
    print(type(song_users))
    pairs = create_pairs(song_users)
    print(len(pairs))
    print(pairs)
    time.sleep(10)
    # response = requests.post(master_url, json=pairs)

app = Flask(__name__)

@app.route('/map', methods = ['POST'])
def get_data():
    data = request.get_json()
    print("received data from master")
    # Filter out records with insufficient elements
    filtered_data = filter(lambda x: len(x.split(';')) >= 3, data)

    # Applying lambda to convert each filtered element to a dictionary
    mapped = list(map(lambda x: {x.split(';')[2]: x.split(';')[0]}, filtered_data))
    for record in mapped:
        shuffle_list[hash(str(record.keys()))%no_workers].append(record)
    mapped_data.append(shuffle_list[int(PORT) - starting_port])
    # for index, worker_url in enumerate(workers_urls):
    #     if worker_url != f"http://localhost:{PORT}/reduce":
    #         print(f"sending to: {worker_url}")
    #         response = requests.post(worker_url, json=shuffle_list[index])
    if len(mapped_data) == no_workers:
        reduce_data()
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
    print("received data from another worker")
    mapped_data.append(data)
    if len(mapped_data) == no_workers:
        reduce_data()
    return make_response('', 200)

@app.route('/aggregate', methods = ['POST'])
def get_aggregate_comand():
    data = request.get_json()
    if data == 'aggregate':
        time.sleep(10)
        response = requests.post(master_url, json=pairs)
    return make_response('', 200)


if __name__ == '__main__':
    print('start')
    if len(sys.argv) != 2:
        print("Provide port number")
        sys.exit(1)
    
    PORT = sys.argv[1]
    mapped_data = []
    shuffle_list = [[],[],[],[]]
    pairs = []
    starting_port = 5000
    no_workers = 4
    master_port = starting_port + no_workers + 1
    master_url = f'http://localhost:{master_port}/aggregate'
    workers_urls = [f'http://localhost:{port_worker}/reduce' for port_worker in range(starting_port, starting_port + no_workers)]
    app.run(debug = True, port = PORT)

    
    """
    TODO: po otrzymaniu komendy zaczynaj nowy wątek w którym będzie wykonywana komenda
    """