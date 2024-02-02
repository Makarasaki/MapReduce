import requests
import csv
from itertools import islice
import requests
from flask import Flask, jsonify, request
import threading
import sys

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

app = Flask(__name__)

@app.route('/aggregate', methods = ['POST'])
def aggregate_data():
    data = request.get_json()
    print("received data from worker")
    aggregated_data.extend(data)
    print(len(aggregated_data))

def run_flask_app(port):
    app.run(debug=True, port=port)

if __name__ == '__main__':
    starting_port = 5000
    no_workers = 4
    workers_map_urls = [f'http://localhost:{port}/map' for port in range(starting_port, starting_port + no_workers)]
    workers_shuffle_urls = [f'http://localhost:{port}/shuffle' for port in range(starting_port, starting_port + no_workers)]
    workers_aggregate_urls = [f'http://localhost:{port}/aggregate' for port in range(starting_port, starting_port + no_workers)]
    master_port = starting_port + no_workers + 1
    aggregated_data = []
    # Create and start the Flask app thread
    flask_thread = threading.Thread(target=run_flask_app, args=(master_port,))
    flask_thread.start()

    data = {'message': 'hi'}
    csv_filename = 'data/spotify_dataset.csv'
    csv_filename = 'data/test.csv' 
    csv_data = csv_to_list(csv_filename)
    # print(csv_data)
    chunks = split_data(csv_data, no_workers)
    # print(chunks)
    # for i in chunks:
    #     print((len(i)))

    for index, worker_url in enumerate(workers_map_urls):
        print(f"Sending raw data to {worker_url}")
        response = requests.post(worker_url, json=chunks[index])
    for index, worker_url in enumerate(workers_shuffle_urls):
        print(f"Sending shuffling comand to {worker_url}")
        response = requests.post(worker_url, json="shuffle")
    for index, worker_url in enumerate(workers_aggregate_urls):
        print(f"Sending shuffling comand to {worker_url}")
        response = requests.post(worker_url, json="aggregate")
    # print(response)
    print("gotowe")