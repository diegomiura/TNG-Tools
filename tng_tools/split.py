import argparse
import csv
import os
import re
import time
from pathlib import Path

import requests
from astropy.io import fits
from astropy.table import Table, vstack


CATALOG_COLUMN_ORDER = [
    'object_id',
    'filename',
    'filter',
    'sim',
    'snapshot',
    'subhalo',
    'dbid',
    'has_merger_row',
    'has_major_past_1gyr',
    'has_major_future_1gyr',
    'has_minor_past_1gyr',
    'has_minor_future_1gyr',
    'has_mini_past_1gyr',
    'has_mini_future_1gyr',
    'major_count_since_1gyr',
    'major_count_until_1gyr',
    'minor_count_since_1gyr',
    'minor_count_until_1gyr',
    'mini_count_since_1gyr',
    'mini_count_until_1gyr',
    'major_time_since_merger',
    'major_time_until_merger',
    'minor_time_since_merger',
    'minor_time_until_merger',
    'mini_time_since_merger',
    'mini_time_until_merger',
]

CATALOG_COLUMN_DTYPES = [
    'i8',
    'U256',
    'U8',
    'i4',
    'i4',
    'i8',
    'U64',
    'bool',
    'bool',
    'bool',
    'bool',
    'bool',
    'bool',
    'bool',
    'i4',
    'i4',
    'i4',
    'i4',
    'i4',
    'i4',
    'f8',
    'f8',
    'f8',
    'f8',
    'f8',
    'f8',
]

DEFAULT_COLUMN_VALUES = {
    'object_id': -1,
    'filename': '',
    'filter': '',
    'sim': -1,
    'snapshot': -1,
    'subhalo': -1,
    'dbid': '',
    'has_merger_row': False,
    'has_major_past_1gyr': False,
    'has_major_future_1gyr': False,
    'has_minor_past_1gyr': False,
    'has_minor_future_1gyr': False,
    'has_mini_past_1gyr': False,
    'has_mini_future_1gyr': False,
    'major_count_since_1gyr': 0,
    'major_count_until_1gyr': 0,
    'minor_count_since_1gyr': 0,
    'minor_count_until_1gyr': 0,
    'mini_count_since_1gyr': 0,
    'mini_count_until_1gyr': 0,
    'major_time_since_merger': -1.0,
    'major_time_until_merger': -1.0,
    'minor_time_since_merger': -1.0,
    'minor_time_until_merger': -1.0,
    'mini_time_since_merger': -1.0,
    'mini_time_until_merger': -1.0,
}

MERGER_LABEL_COLUMNS = [
    'has_major_past_1gyr',
    'has_major_future_1gyr',
    'has_minor_past_1gyr',
    'has_minor_future_1gyr',
    'has_mini_past_1gyr',
    'has_mini_future_1gyr',
    'major_count_since_1gyr',
    'major_count_until_1gyr',
    'minor_count_since_1gyr',
    'minor_count_until_1gyr',
    'mini_count_since_1gyr',
    'mini_count_until_1gyr',
    'major_time_since_merger',
    'major_time_until_merger',
    'minor_time_since_merger',
    'minor_time_until_merger',
    'mini_time_since_merger',
    'mini_time_until_merger',
]

SPLIT_FILENAME_RE = re.compile(
    r'^(?P<sim>\d+)_(?P<snapshot>\d+)_(?P<subhalo>\d+)_(?P<filter>[A-Za-z])_(?P<version>v\d+|v\?)_hsc_realistic\.fits$'
)


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=-1.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _default_catalog_entry():
    return dict(DEFAULT_COLUMN_VALUES)


def _default_value_for_column(column_name):
    return DEFAULT_COLUMN_VALUES.get(column_name, '')


def _merger_labels_from_row(row):
    labels = {col: DEFAULT_COLUMN_VALUES[col] for col in MERGER_LABEL_COLUMNS}

    major_count_since = _safe_int(row.get('Major_CountSince1Gyr'))
    major_count_until = _safe_int(row.get('Major_CountUntil1Gyr'))
    minor_count_since = _safe_int(row.get('Minor_CountSince1Gyr'))
    minor_count_until = _safe_int(row.get('Minor_CountUntil1Gyr'))
    mini_count_since = _safe_int(row.get('Mini_CountSince1Gyr'))
    mini_count_until = _safe_int(row.get('Mini_CountUntil1Gyr'))

    labels.update({
        'has_major_past_1gyr': major_count_since > 0,
        'has_major_future_1gyr': major_count_until > 0,
        'has_minor_past_1gyr': minor_count_since > 0,
        'has_minor_future_1gyr': minor_count_until > 0,
        'has_mini_past_1gyr': mini_count_since > 0,
        'has_mini_future_1gyr': mini_count_until > 0,
        'major_count_since_1gyr': major_count_since,
        'major_count_until_1gyr': major_count_until,
        'minor_count_since_1gyr': minor_count_since,
        'minor_count_until_1gyr': minor_count_until,
        'mini_count_since_1gyr': mini_count_since,
        'mini_count_until_1gyr': mini_count_until,
        'major_time_since_merger': _safe_float(row.get('Major_TimeSinceMerger')),
        'major_time_until_merger': _safe_float(row.get('Major_TimeUntilMerger')),
        'minor_time_since_merger': _safe_float(row.get('Minor_TimeSinceMerger')),
        'minor_time_until_merger': _safe_float(row.get('Minor_TimeUntilMerger')),
        'mini_time_since_merger': _safe_float(row.get('Mini_TimeSinceMerger')),
        'mini_time_until_merger': _safe_float(row.get('Mini_TimeUntilMerger')),
    })
    return labels


def _candidate_merger_csv_paths(sim, merger_history_dir=None, split_output_dir=None):
    filename = f'Mergers_TNG{sim}-1.csv'
    candidates = []

    if merger_history_dir is not None:
        base = Path(merger_history_dir).expanduser()
        if base.is_file():
            candidates.append(base)
        else:
            candidates.extend([
                base / filename,
                base / 'merger_history' / filename,
                base / 'ConnorBottrellData' / filename,
            ])
    else:
        roots = [Path.cwd(), Path.cwd().parent]
        if split_output_dir:
            try:
                split_root = Path(split_output_dir).expanduser().resolve().parent
                roots.extend([split_root, split_root.parent])
            except OSError:
                pass

        for root in roots:
            candidates.extend([
                root / filename,
                root / 'merger_history' / filename,
                root / 'ConnorBottrellData' / filename,
            ])

    deduped = []
    seen = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _resolve_merger_csv_path(sim, merger_history_dir=None, split_output_dir=None):
    for path in _candidate_merger_csv_paths(
        sim=sim,
        merger_history_dir=merger_history_dir,
        split_output_dir=split_output_dir,
    ):
        if path.exists():
            return path
    return None


def _load_merger_rows(sim, merger_history_dir=None, split_output_dir=None):
    path = _resolve_merger_csv_path(
        sim=sim,
        merger_history_dir=merger_history_dir,
        split_output_dir=split_output_dir,
    )
    if path is None:
        print(f' ⚠️  merger history CSV not found for TNG{sim}; merger labels default to empty')
        return {}

    merger_rows = {}
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dbid = row.get('dbID')
            if not dbid:
                continue
            merger_rows[dbid] = _merger_labels_from_row(row)

    print(f' 📄 loaded merger history for TNG{sim}: {len(merger_rows)} rows from {path}')
    return merger_rows


def _build_catalog_entry(
    sim,
    snapshot,
    subhalo,
    filt,
    filename,
    merger_cache,
    merger_history_dir=None,
    split_output_dir=None,
):
    sim_int = _safe_int(sim, default=-1)
    snapshot_int = _safe_int(snapshot, default=-1)
    subhalo_int = _safe_int(subhalo, default=-1)
    dbid = ''
    if snapshot_int >= 0 and subhalo_int >= 0:
        dbid = f'{snapshot_int}_{subhalo_int}'

    entry = _default_catalog_entry()
    entry.update({
        'object_id': snapshot_int * 1000000 + subhalo_int if snapshot_int >= 0 and subhalo_int >= 0 else -1,
        'filename': filename,
        'filter': str(filt),
        'sim': sim_int,
        'snapshot': snapshot_int,
        'subhalo': subhalo_int,
        'dbid': dbid,
    })

    if sim_int < 0 or not dbid:
        return entry

    sim_key = str(sim_int)
    if sim_key not in merger_cache:
        merger_cache[sim_key] = _load_merger_rows(
            sim=sim_key,
            merger_history_dir=merger_history_dir,
            split_output_dir=split_output_dir,
        )

    merger_labels = merger_cache[sim_key].get(dbid)
    if merger_labels:
        entry['has_merger_row'] = True
        for key in CATALOG_COLUMN_ORDER:
            if key in merger_labels:
                entry[key] = merger_labels[key]
    return entry


def _parse_split_filename(filename):
    match = SPLIT_FILENAME_RE.match(str(filename))
    if match is None:
        return None
    return {
        'sim': match.group('sim'),
        'snapshot': match.group('snapshot'),
        'subhalo': match.group('subhalo'),
        'filter': match.group('filter'),
    }


def _fill_missing_columns(table, all_columns):
    for col in all_columns:
        if col not in table.colnames:
            table[col] = [_default_value_for_column(col)] * len(table)


def _upgrade_existing_catalog(existing, merger_cache, merger_history_dir=None, split_output_dir=None):
    if 'filename' not in existing.colnames:
        return existing

    parsed_entries = []
    filters = existing['filter'] if 'filter' in existing.colnames else [''] * len(existing)
    for filename, filt in zip(existing['filename'], filters):
        parsed = _parse_split_filename(filename)
        if parsed is None:
            entry = _default_catalog_entry()
            entry['filename'] = str(filename)
            entry['filter'] = str(filt)
        else:
            entry = _build_catalog_entry(
                sim=parsed['sim'],
                snapshot=parsed['snapshot'],
                subhalo=parsed['subhalo'],
                filt=filt or parsed['filter'],
                filename=str(filename),
                merger_cache=merger_cache,
                merger_history_dir=merger_history_dir,
                split_output_dir=split_output_dir,
            )
        parsed_entries.append(entry)

    for col in CATALOG_COLUMN_ORDER:
        if col not in existing.colnames:
            existing[col] = [entry[col] for entry in parsed_entries]

    return existing


def _build_catalog_table(catalog_entries):
    if len(catalog_entries) == 0:
        return Table(names=CATALOG_COLUMN_ORDER, dtype=CATALOG_COLUMN_DTYPES)
    rows = [[entry[col] for col in CATALOG_COLUMN_ORDER] for entry in catalog_entries]
    return Table(rows=rows, names=CATALOG_COLUMN_ORDER)


def _write_catalog_table(
    table,
    catalog_path,
    catalog_append=False,
    merger_cache=None,
    merger_history_dir=None,
    split_output_dir=None,
):
    if merger_cache is None:
        merger_cache = {}

    if catalog_append and os.path.exists(catalog_path):
        existing = Table.read(catalog_path)
        existing = _upgrade_existing_catalog(
            existing,
            merger_cache=merger_cache,
            merger_history_dir=merger_history_dir,
            split_output_dir=split_output_dir,
        )

        existing_filenames = {str(fn) for fn in existing['filename']}
        if len(table) > 0:
            mask = [str(fn) not in existing_filenames for fn in table['filename']]
            table = table[mask]

        all_columns = list(dict.fromkeys(list(existing.colnames) + list(table.colnames)))
        _fill_missing_columns(existing, all_columns)
        _fill_missing_columns(table, all_columns)
        existing = existing[all_columns]
        table = table[all_columns]

        if len(table) > 0:
            table = vstack([existing, table])
        else:
            table = existing

    table.write(catalog_path, overwrite=True)
    print(f' 📄 wrote catalog with {len(table)} entries to {catalog_path}')


def build_catalog_from_split_images(
    split_output_dir='split_images',
    catalog_path=None,
    catalog_append=False,
    merger_history_dir=None,
    recursive=False,
):
    split_dir = Path(split_output_dir)
    if not split_dir.exists():
        raise FileNotFoundError(f'split directory does not exist: {split_output_dir}')

    if catalog_path is None:
        catalog_path = os.path.join(split_output_dir, 'catalog.fits')

    pattern = '*_hsc_realistic.fits'
    files_iter = split_dir.rglob(pattern) if recursive else split_dir.glob(pattern)

    merger_cache = {}
    catalog_entries = []
    seen_filenames = set()
    duplicates = 0
    scanned = 0

    for file_path in sorted(files_iter):
        scanned += 1
        filename = file_path.name
        if filename in seen_filenames:
            duplicates += 1
            continue
        seen_filenames.add(filename)

        parsed = _parse_split_filename(filename)
        if parsed is None:
            continue

        catalog_entries.append(
            _build_catalog_entry(
                sim=parsed['sim'],
                snapshot=parsed['snapshot'],
                subhalo=parsed['subhalo'],
                filt=parsed['filter'],
                filename=filename,
                merger_cache=merger_cache,
                merger_history_dir=merger_history_dir,
                split_output_dir=split_output_dir,
            )
        )

    table = _build_catalog_table(catalog_entries)
    _write_catalog_table(
        table=table,
        catalog_path=catalog_path,
        catalog_append=catalog_append,
        merger_cache=merger_cache,
        merger_history_dir=merger_history_dir,
        split_output_dir=split_output_dir,
    )
    print(
        f' 📊 catalog build scanned={scanned} parsed={len(catalog_entries)} '
        f'duplicate_names={duplicates} recursive={recursive}'
    )
    return table


def download_and_split_hsc_images(
    split_output_dir='split_images',
    URL_LIST=None,
    BATCH_START=None,
    BATCH_SIZE=None,
    API_KEY=None,
    remove_parent: bool = True,
    catalog_path=None,
    parent_file_only: bool = False,
    parent_output_dir: str = None,
    failed_urls_path: str = None,
    max_retries: int = 3,
    retry_backoff_sec: float = 2.0,
    catalog_append: bool = False,
    merger_history_dir: str = None,
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
        remove_parent (bool, optional): If True, delete the original downloaded FITS file after splitting. Defaults to True.
        catalog_path (str, optional): If provided, saves a Hyrax-compatible FITS catalog at this location.
            When used from the CLI, defaults to split_output_dir/catalog.fits.
            The catalog includes image identifiers plus merger-history labels by default.
        parent_file_only (bool, optional): If True, only download the parent FITS files and skip splitting and catalog creation. Defaults to False.
        parent_output_dir (str, optional): Directory to save downloaded parent FITS files.
            If None, uses split_output_dir. Defaults to None.
        failed_urls_path (str, optional): If provided, write failed URLs (with error) here.
            When used from the CLI, defaults to split_output_dir/failed_urls.txt.
        max_retries (int, optional): Number of download attempts per URL. Defaults to 3.
        retry_backoff_sec (float, optional): Base seconds to wait between retries. Defaults to 2.0.
        catalog_append (bool, optional): If True and catalog exists, append new entries without
            duplicating filenames. Defaults to False.
        merger_history_dir (str, optional): Directory or file path for merger CSV files.
            If not provided, the tool auto-detects common locations.

    Notes:
        - Split FITS images will be named as: SIM_SNAPSHOT_SUBHALO_FILTER_VERSION_hsc_realistic.fits
          (e.g., 50_72_0_G_v2_hsc_realistic.fits). If no version is parsed, 'v?' is used.
        - Catalog format is compatible with Hyrax's FitsImageDataSet expectations.
        - The 'object_id' in the catalog is computed as (int(snapshot) * 1_000_000) + int(subhalo).
        - Merger labels are looked up from Mergers_TNG50-1.csv / Mergers_TNG100-1.csv by dbID=snapshot_subhalo.

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
    if BATCH_START is None:
        BATCH_START = 0
    if BATCH_SIZE is None:
        BATCH_SIZE = max(len(urls) - BATCH_START, 0)
    batch = urls[BATCH_START : BATCH_START + BATCH_SIZE]

    catalog_entries = [] if catalog_path else None
    failed_urls = [] if failed_urls_path else None
    merger_cache = {}

    # helper to pull sim, snapshot, subhalo, version from URL
    def parse_url(u):
        parts = u.split('/')
        sim_token = parts[4]      # e.g. 'TNG50-1'
        sim_match = re.search(r'TNG(\d+)', sim_token)
        sim = sim_match.group(1) if sim_match else 'TNG?'
        snapshot = parts[6]       # e.g. '72'
        subhalo = parts[8]        # e.g. '0'
        fn = parts[-1]       # e.g. 'skirt_images_hsc_realistic_v2.fits'
        v_match = re.search(r'(v\d+)', fn)
        version = v_match.group(1) if v_match else 'v?'
        return sim, snapshot, subhalo, version

    def record_failure(url, err):
        if failed_urls is not None:
            failed_urls.append(f'{url}\t{err}')

    def download_with_retries(url):
        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get(url, headers={'API-Key': API_KEY}, stream=True)
                r.raise_for_status()
                return r
            except requests.RequestException as exc:
                last_err = exc
                if attempt < max_retries:
                    time.sleep(retry_backoff_sec * attempt)
        raise last_err

    # main loop
    for url in batch:
        sim, snapshot, subhalo, version = parse_url(url)

        # download parent file
        fname_parent = f'{sim}_{snapshot}_{subhalo}_{version}_parent.fits'
        parent_path = os.path.join(parent_dir, fname_parent)
        print(f'\nDownloading {fname_parent} into {parent_dir} …')
        try:
            r = download_with_retries(url)
            with open(parent_path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        except Exception as exc:
            print(f' ⚠️  download failed for {fname_parent}: {exc}')
            record_failure(url, exc)
            continue

        if not parent_file_only:
            # open and split
            try:
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
                        out_name = f'{sim}_{snapshot}_{subhalo}_{filt}_{version}_hsc_realistic.fits'
                        out_path = os.path.join(split_output_dir, out_name)
                        new_hdu.writeto(out_path, overwrite=True)
                        print(f' ✅ wrote {out_name}')
                        if catalog_entries is not None:
                            catalog_entries.append(
                                _build_catalog_entry(
                                    sim=sim,
                                    snapshot=snapshot,
                                    subhalo=subhalo,
                                    filt=filt,
                                    filename=out_name,
                                    merger_cache=merger_cache,
                                    merger_history_dir=merger_history_dir,
                                    split_output_dir=split_output_dir,
                                )
                            )
            except Exception as exc:
                print(f' ⚠️  split failed for {fname_parent}: {exc}')
                record_failure(url, exc)
                continue

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
        table = _build_catalog_table(catalog_entries)
        _write_catalog_table(
            table=table,
            catalog_path=catalog_path,
            catalog_append=catalog_append,
            merger_cache=merger_cache,
            merger_history_dir=merger_history_dir,
            split_output_dir=split_output_dir,
        )

    if failed_urls is not None:
        failed_dir = os.path.dirname(failed_urls_path)
        if failed_dir:
            os.makedirs(failed_dir, exist_ok=True)
        with open(failed_urls_path, 'w') as f:
            for line in failed_urls:
                f.write(line + '\n')
        print(f' 📄 wrote failed URL list to {failed_urls_path}')


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
    gen_parser.add_argument('--sim', choices=['tng50', 'tng100'], default='tng50',
                            help='Simulation to query (default: tng50)')
    gen_parser.add_argument('--snapshot', type=int, choices=[72, 91], default=91,
                            help='Snapshot number to filter (default: 91)')

    # Split sub-command
    split_parser = sub.add_parser('split', help='Download & split HSC FITS images')
    split_parser.add_argument('--url-list',    required=True,
                              help='Path to the text file of URLs')
    split_parser.add_argument('--split-output-dir',  default='split_images',
                              help='Directory for split FITS images')
    split_parser.add_argument('--batch-start', type=int, default=0,
                              help='Starting index for URL batch')
    split_parser.add_argument('--batch-size',  type=int, default=None,
                              help='Number of URLs to process (default: all)')
    split_parser.add_argument('--api-key', help='Your TNG50-1 API key (or set via .env)')
    parent_group = split_parser.add_mutually_exclusive_group()
    parent_group.add_argument('--remove-parent', action='store_true', default=True,
                              help='Remove parent FITS after splitting (default)')
    parent_group.add_argument('--keep-parent', action='store_false', dest='remove_parent',
                              help='Keep parent FITS after splitting')
    split_parser.add_argument('--catalog-path',
                              help='Path to save Hyrax-compatible FITS catalog (default: split_output_dir/catalog.fits)')
    split_parser.add_argument('--catalog-append', action='store_true',
                              help='Append to existing catalog and avoid duplicate filenames')
    split_parser.add_argument('--parent-only', action='store_true',
                              help='Only download parent FITS files')
    split_parser.add_argument('--failed-urls', default=None,
                              help='Write failed URLs (with errors) to this file (default: split_output_dir/failed_urls.txt)')
    split_parser.add_argument('--max-retries', type=int, default=3,
                              help='Number of download attempts per URL')
    split_parser.add_argument('--retry-backoff-sec', type=float, default=2.0,
                              help='Base seconds to wait between retries')
    split_parser.add_argument('--merger-history-dir', default=None,
                              help='Directory containing merger CSVs (default: auto-detect)')

    # Catalog sub-command (no downloads)
    catalog_parser = sub.add_parser('catalog', help='Build/refresh catalog from existing split FITS files')
    catalog_parser.add_argument('--split-output-dir', default='split_images',
                                help='Directory containing existing split FITS images')
    catalog_parser.add_argument('--catalog-path',
                                help='Path to save catalog (default: split_output_dir/catalog.fits)')
    catalog_parser.add_argument('--catalog-append', action='store_true',
                                help='Append to existing catalog and avoid duplicate filenames')
    catalog_parser.add_argument('--merger-history-dir', default=None,
                                help='Directory containing merger CSVs (default: auto-detect)')
    catalog_parser.add_argument('--recursive', action='store_true',
                                help='Search split FITS files recursively under split-output-dir')

    args = parser.parse_args()

    if args.command == 'gen-urls':
        api_key = args.api_key or os.getenv('TNG50_API_KEY')
        if not api_key:
            parser.error('An API key is required: pass --api-key or set TNG50_API_KEY in .env')
        from tng_tools.fetch import make_list_of_urls
        sim_api = args.sim.upper() + '-1'
        ending = f'/api/{sim_api}/files/skirt_images_hsc/'
        snapshot_filter = f'_realistic_v2_{args.snapshot}'
        make_list_of_urls(API_KEY=api_key, ENDING=ending, SNAPSHOT_FILTER=snapshot_filter)
        # move default output if custom path requested
        if args.output != 'all_file_urls.txt':
            os.replace('all_file_urls.txt', args.output)
        print(f' 📄 wrote URL list to {args.output}')
    elif args.command == 'split':
        api_key = args.api_key or os.getenv('TNG50_API_KEY')
        if not api_key:
            parser.error('An API key is required: pass --api-key or set TNG50_API_KEY in .env')
        if args.catalog_path is None and not args.parent_only:
            args.catalog_path = os.path.join(args.split_output_dir, 'catalog.fits')
        if args.failed_urls is None:
            args.failed_urls = os.path.join(args.split_output_dir, 'failed_urls.txt')
        download_and_split_hsc_images(
            split_output_dir=args.split_output_dir,
            URL_LIST=args.url_list,
            BATCH_START=args.batch_start,
            BATCH_SIZE=args.batch_size,
            API_KEY=api_key,
            remove_parent=args.remove_parent,
            catalog_path=args.catalog_path,
            parent_file_only=args.parent_only,
            failed_urls_path=args.failed_urls,
            max_retries=args.max_retries,
            retry_backoff_sec=args.retry_backoff_sec,
            catalog_append=args.catalog_append,
            merger_history_dir=args.merger_history_dir,
        )
    elif args.command == 'catalog':
        if args.catalog_path is None:
            args.catalog_path = os.path.join(args.split_output_dir, 'catalog.fits')
        build_catalog_from_split_images(
            split_output_dir=args.split_output_dir,
            catalog_path=args.catalog_path,
            catalog_append=args.catalog_append,
            merger_history_dir=args.merger_history_dir,
            recursive=args.recursive,
        )
