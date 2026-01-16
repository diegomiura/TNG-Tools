import os
import time
import requests
import re
from astropy.io import fits
from astropy.table import Table
import argparse

def download_and_split_hsc_images(
    split_output_dir='split_images',
    URL_LIST=None,
    BATCH_START=None,
    BATCH_SIZE=None,
    API_KEY=None,
    remove_parent: bool = False,
    catalog_path=None,
    parent_file_only: bool = False,
    parent_output_dir: str = None
):
    '''
    Downloads and splits HSC survey FITS images from the TNG50-1 API into individual filters,
    optionally removes the original parent FITS files, and can generate a catalog compatible with Hyrax.

    Args:
        split_output_dir (str, optional): Directory to save split FITS images. Defaults to 'split_images'.
        URL_LIST (str): Path to a text file containing one URL per line.
        BATCH_START (int): Starting index for the batch of URLs to download.
        BATCH_SIZE (int): Number of URLs to process in this batch.
        API_KEY (str): API key required to access the TNG50-1 API.
        remove_parent (bool, optional): If True, delete the original downloaded FITS file after splitting. Defaults to False.
        catalog_path (str, optional): If provided, saves a Hyrax-compatible FITS catalog at this location.
            The catalog will include columns: 'object_id' (an integer composed of snapshot and subhalo), 'filename', and 'filter'.
        parent_file_only (bool, optional): If True, only download the parent FITS files and skip splitting and catalog creation. Defaults to False.
        parent_output_dir (str, optional): Directory to save downloaded parent FITS files.
            If None, uses split_output_dir. Defaults to None.

    Notes:
        - Split FITS images will be named as: SNAPSHOT_SUBHALO_FILTER_VERSION_hsc_realistic.fits
          (e.g., 72_0_G_v2_hsc_realistic.fits). If no version is parsed, 'v?' is used.
        - Catalog format is compatible with Hyrax's FitsImageDataSet expectations.
        - The 'object_id' in the catalog is computed as (int(snapshot) * 1_000_000) + int(subhalo).

    Example:
        # Save split images and keep the original FITS files
        download_and_split_hsc_images(
            split_output_dir='split_images',
            URL_LIST='urls.txt',
            BATCH_START=0,
            BATCH_SIZE=50,
            API_KEY='YOUR_API_KEY'
        )

        # Save split images, remove the parent file, and write a catalog
        download_and_split_hsc_images(
            split_output_dir='split_images',
            URL_LIST='urls.txt',
            BATCH_START=0,
            BATCH_SIZE=50,
            API_KEY='YOUR_API_KEY',
            remove_parent=True,
            catalog_path='split_images/catalog.fits'
        )

        # Download only the parent files, no splitting or catalog
        download_and_split_hsc_images(
            URL_LIST='urls.txt',
            BATCH_START=0,
            BATCH_SIZE=10,
            API_KEY='YOUR_API_KEY',
            parent_file_only=True
        )
    '''
    # determine directory for parent files
    parent_dir = parent_output_dir or split_output_dir
    os.makedirs(parent_dir, exist_ok=True)

    # ensure output dir exists
    if not parent_file_only:
        os.makedirs(split_output_dir, exist_ok=True)

    # load URLs and pick batch
    with open(URL_LIST) as f:
        urls = [u.strip() for u in f if u.strip()]
    batch = urls[BATCH_START : BATCH_START + BATCH_SIZE]

    catalog_entries = [] if catalog_path else None

    # helper to pull snapshot, subhalo, version from URL
    def parse_url(u):
        parts = u.split('/')
        snapshot = parts[6]        # e.g. '72'
        subhalo = parts[8]        # e.g. '0'
        fn = parts[-1]       # e.g. 'skirt_images_hsc_realistic_v2.fits'
        v_match = re.search(r'(v\d+)', fn)
        version = v_match.group(1) if v_match else 'v?'
        return snapshot, subhalo, version

    # main loop
    for url in batch:
        snapshot, subhalo, version = parse_url(url)

        # download parent file
        fname_parent = f'{snapshot}_{subhalo}_{version}_parent.fits'
        parent_path = os.path.join(parent_dir, fname_parent)
        print(f'\nDownloading {fname_parent} into {parent_dir} …')
        r = requests.get(url, headers={'API-Key': API_KEY}, stream=True)
        r.raise_for_status()
        with open(parent_path, 'wb') as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

        if not parent_file_only:
            # open and split
            with fits.open(parent_path, memmap=True) as hdul:
                for filt in ['G', 'R', 'I', 'Z', 'Y']:
                    target_ext = f'SUBARU_HSC.{filt}'
                    sci_hdu = next(
                        (h for h in hdul if h.header.get('EXTNAME','') == target_ext),
                        None
                    )
                    if sci_hdu is None:
                        print(f' ⚠️  no extension {target_ext} in {fname_parent}')
                        continue

                    new_hdu = fits.PrimaryHDU(data=sci_hdu.data, header=sci_hdu.header)
                    out_name = f'{snapshot}_{subhalo}_{filt}_{version}_hsc_realistic.fits'
                    out_path = os.path.join(split_output_dir, out_name)
                    new_hdu.writeto(out_path, overwrite=True)
                    print(f' ✅ wrote {out_name}')
                    if catalog_entries is not None:
                        # construct 8-digit object_id: snapshot (2 digits) + subhalo (6-digit zero-padded)
                        obj_id = int(snapshot) * 1000000 + int(subhalo)
                        catalog_entries.append({
                            'object_id': obj_id,
                            'filename': out_name,
                            'filter': filt
                        })

            # optionally remove parent file
            if remove_parent:
                try:
                    os.remove(parent_path)
                    print(f' 🗑 removed parent file {fname_parent}')
                except OSError as e:
                    print(f' ⚠️  could not remove {fname_parent}: {e}')

            # be gentle on the API server
            time.sleep(1)

    if catalog_entries is not None and not parent_file_only:
        table = Table(rows=catalog_entries, names=['object_id', 'filename', 'filter'])
        table.write(catalog_path, overwrite=True)
        print(f' 📄 wrote catalog with {len(catalog_entries)} entries to {catalog_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Download & split HSC images from the TNG50-1 API'
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # Gen-urls sub-command
    gen_parser = sub.add_parser('gen-urls', help='Fetch HSC FITS URLs into a text file')
    gen_parser.add_argument('--api-key', help='TNG50-1 API key (or set via .env)')
    gen_parser.add_argument('--output', default='all_file_urls.txt',
                            help='Where to write the URL list')

    # Split sub-command
    split_parser = sub.add_parser('split', help='Download & split HSC FITS images')
    split_parser.add_argument('--url-list',    required=True,
                              help='Path to the text file of URLs')
    split_parser.add_argument('--split-output-dir',  default='split_images',
                              help='Directory for split FITS images')
    split_parser.add_argument('--batch-start', type=int, default=0,
                              help='Starting index for URL batch')
    split_parser.add_argument('--batch-size',  type=int, required=True,
                              help='Number of URLs to process')
    split_parser.add_argument('--api-key', help='Your TNG50-1 API key (or set via .env)')
    split_parser.add_argument('--remove-parent', action='store_true',
                              help='Remove parent FITS after splitting')
    split_parser.add_argument('--catalog-path',
                              help='Path to save Hyrax-compatible FITS catalog')
    split_parser.add_argument('--parent-only', action='store_true',
                              help='Only download parent FITS files')

    args = parser.parse_args()

    # Resolve API key from flag or environment
    api_key = args.api_key or os.getenv('TNG50_API_KEY')
    if not api_key:
        parser.error('An API key is required: pass --api-key or set TNG50_API_KEY in .env')

    if args.command == 'gen-urls':
        from tng_tools.fetch import make_list_of_urls
        make_list_of_urls(API_KEY=api_key)
        # move default output if custom path requested
        if args.output != 'all_file_urls.txt':
            os.replace('all_file_urls.txt', args.output)
        print(f' 📄 wrote URL list to {args.output}')
    elif args.command == 'split':
        download_and_split_hsc_images(
            split_output_dir=args.split_output_dir,
            URL_LIST=args.url_list,
            BATCH_START=args.batch_start,
            BATCH_SIZE=args.batch_size,
            API_KEY=api_key,
            remove_parent=args.remove_parent,
            catalog_path=args.catalog_path,
            parent_file_only=args.parent_only
        )
