from google.cloud import bigquery
import os
import sys

client = bigquery.Client()
project = client.project
print(f'Project: {project}')

datasets = list(client.list_datasets())
print(f'Datasets: {[d.dataset_id for d in datasets]}')
