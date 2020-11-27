import os
import sys
import requests
from hurry.filesize import size

def get_root():
    path = os.getcwd()    
    root = False
    i = 0

    while not root:
        if not os.path.exists(os.path.join(path, 'requirements.txt')):
            path = os.path.abspath(os.path.join(path, os.pardir))
        else:
            root = True
        i = i+1

    return path
    
cwd = get_root()
DATASOURCE_PATH = r'https://stdatalake009.blob.core.windows.net/public/temperatures.csv'
DATA_IN_FOLDER = os.path.join(cwd, 'data', 'in')
DATA_CURATED_FOLDER = os.path.join(cwd, 'data', 'curated')
DATA_OUT_FOLDER = os.path.join(cwd, 'data', 'out')
DATA_REPORTS_FOLDER = os.path.join(cwd, 'data', 'reports')
FILE_NAME_IN = 'temperatures.csv'
MAP_FILE_IN = 'natural-earth-countries-1_50m.shp'
MAP_FILE_OUT = 'world.png'


def initialize_referential():
    global_datasource_dest = os.path.join(DATA_IN_FOLDER, FILE_NAME_IN)

    if os.path.exists(global_datasource_dest):
        print('Data already downloaded')
    else :
        print('Download %s to %s' % (DATASOURCE_PATH, DATA_IN_FOLDER))
        with open(global_datasource_dest, 'wb') as f:
            response = requests.get(DATASOURCE_PATH, stream=True)
            total_length = response.headers.get('content-length')
        
            if total_length is None: # no content length header
                f.write(response.content)
            else:
                dl = 0
                total_length = int(total_length)
                print("Downloading %s" % (global_datasource_dest))
                for data in response.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(100 * dl / total_length)
                    sys.stdout.write("\r[%s%s] %s/%s" % ('=' * done, ' ' * (100-done), size(dl), size(total_length))) 
                    sys.stdout.flush()

            print('', end = "\r\n")
            print('Data downloaded')
