import csv
import requests
import requests
import threading
from flask import Flask, jsonify, request, make_response

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

def csv_to_list(filename):
    with open(filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip('"\n'))
        return data

def final_results(results):
    for result in results:
        for user1, user2 in result.items():
            print(f"User '{user1}' should follow user '{user2}'")

app = Flask(__name__)

@app.route('/aggregate', methods = ['POST'])
def aggregate_data():
    data = request.get_json()
    print(f"Received data from worker, size: {len(data)}")
    aggregated_data.append(data)
    if len(aggregated_data) == no_workers:
        final_results(aggregated_data)
    return make_response('', 200)

@app.route('/controller', methods = ['POST'])
def workflow_control():
    data = request.get_json()
    ready_counter[data] += 1
    if ready_counter['ready for shuffling'] == no_workers:
        ready_counter['ready for shuffling'] = 0
        send_comand("shuffle", workers_shuffle_urls)
    elif ready_counter['ready for shuffling 2'] == no_workers:
        ready_counter['ready for shuffling 2'] = 0
        send_comand("shuffle 2", workers_shuffle_urls)
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

if __name__ == '__main__':
    starting_port = 5000
    no_workers = 4
    workers_map_urls = [f'http://localhost:{port}/map' for port in range(starting_port, starting_port + no_workers)]
    workers_shuffle_urls = [f'http://localhost:{port}/shuffle' for port in range(starting_port, starting_port + no_workers)]
    workers_reduce_urls = [f'http://localhost:{port}/reduce' for port in range(starting_port, starting_port + no_workers)]
    workers_aggregate_urls = [f'http://localhost:{port}/aggregate' for port in range(starting_port, starting_port + no_workers)]
    master_port = starting_port + no_workers + 1
    aggregated_data = []
    ready_counter = {'ready for shuffling': 0, 'ready for shuffling 2': 0}
    # csv_filename = 'data/spotify_dataset.csv'
    csv_filename = 'data/test.csv' 
    csv_data = csv_to_list(csv_filename)
    chunks = split_data(csv_data, no_workers)

    main_thread = threading.Thread(target=send_data)
    main_thread.start()

    run_flask_app(master_port)
