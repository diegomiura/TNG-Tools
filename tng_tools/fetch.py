import os
import requests
from dotenv import load_dotenv
load_dotenv()

def make_list_of_urls(API_KEY=None,
                      BASE_API_URL='https://www.tng-project.org/',
                      ENDING='/api/TNG50-1/files/skirt_images_hsc/',
                      SNAPSHOT_FILTER='_realistic_v2_91'):
    '''
    Fetches all FITS image URLs from the TNG50-1 API, filters by snapshot tag,
    and writes them to a text file.

    Args:
        API_KEY (str): API key for authenticating with the TNG50-1 API (required).
        BASE_API_URL (str, optional): Base URL of the TNG API.
        ENDING (str, optional): API endpoint path for HSC FITS files.
        SNAPSHOT_FILTER (str, optional): Substring to filter snapshot URLs.

    Writes:
        'all_file_urls.txt' in the current directory, containing one URL per line.
    '''
    # Retrive data from a given API endpoint
    def get_endpoint(url):
        r = requests.get(url, headers={'API-Key': API_KEY})
        r.raise_for_status()
        return r.json()

    snapshot_urls = get_endpoint(BASE_API_URL + ENDING)
    filtered = [u for u in snapshot_urls if SNAPSHOT_FILTER in u]
    all_urls = []
    for url in filtered:
        all_urls += get_endpoint(url)['files']

    with open('all_file_urls.txt', 'w') as f:
        for u in all_urls:
            f.write(u + '\n')

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Fetch HSC FITS URLs into a text file'
    )
    parser.add_argument('--api-key', help='TNG50-1 API key (or set via .env)')
    parser.add_argument('--output', default='all_file_urls.txt',
                        help='Where to write the URL list')
    args = parser.parse_args()

    api_key = args.api_key or os.getenv('TNG50_API_KEY')
    if not api_key:
        parser.error('An API key is required: pass --api-key or set TNG50_API_KEY in .env')
    make_list_of_urls(API_KEY=api_key)
    if args.output != 'all_file_urls.txt':
        os.replace('all_file_urls.txt', args.output)
    print(f' 📄 wrote URL list to {args.output}')
