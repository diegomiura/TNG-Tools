# TNG Tools

CLI tools to fetch and split HSC FITS images from the IllustrisTNG simulations.

## Installation

```bash
pip install .
```

## Usage

Recommended unified CLI:

### Fetch HSC FITS URLs

```bash
tng-tools gen-urls --sim tng50 --snapshot 91 --api-key YOUR_API_KEY --output urls.txt
```

This command fetches all FITS image URLs from the TNG50-1 API filtered by snapshot and writes them to `urls.txt`.
An API key is required (pass `--api-key` or set `TNG50_API_KEY`).

### Download and split HSC FITS images

```bash
tng-tools split --url-list urls.txt --split-output-dir split_images --api-key YOUR_API_KEY
```

This command downloads the FITS files listed in `urls.txt` (by default, all remaining), splits them by filter, and saves the split images.

Legacy commands (still supported):

```bash
tng-gen-urls --sim tng50 --snapshot 91 --api-key YOUR_API_KEY --output urls.txt
tng-split gen-urls --sim tng50 --snapshot 91 --api-key YOUR_API_KEY --output urls.txt
tng-split split --url-list urls.txt --split-output-dir split_images --api-key YOUR_API_KEY
```

Note: the legacy commands are still supported, but `tng-tools` is the preferred entry point going forward.

Additional options:

- `--split-output-dir`: Directory to save split images (default: `split_images`).
- `--batch-start`: Starting index for the batch (default: 0).
- `--batch-size`: Number of URLs to process (default: all remaining).
- `--remove-parent`: Remove original downloaded FITS files after splitting (default).
- `--keep-parent`: Keep parent FITS files after splitting.
- `--catalog-path`: Path to save a Hyrax-compatible FITS catalog (default: `split_images/catalog.fits`).
- `--catalog-append`: Append to an existing catalog and avoid duplicate filenames.
- `--parent-only`: Only download parent FITS files without splitting or catalog creation.
- `--failed-urls`: Write failed URLs (with errors) to this file (default: `split_images/failed_urls.txt`).
- `--max-retries`: Number of download attempts per URL (default: 3).
- `--retry-backoff-sec`: Base seconds to wait between retries (default: 2.0).

Notes:

- Split files are named like `SIM_SNAPSHOT_SUBHALO_FILTER_VERSION_hsc_realistic.fits`
  (for example, `50_72_0_G_v2_hsc_realistic.fits`). If no version is parsed, `v?` is used.
- Catalog `object_id` is computed as `int(snapshot) * 1000000 + int(subhalo)`.
- To retry failed downloads later and extend the catalog, re-run with the failed URL list
  and `--catalog-append`.

## Environment Variables

You can also set your API key in a `.env` file in the root directory:

```
TNG50_API_KEY=your_api_key_here
```

Make sure to add `.env` to your `.gitignore` to avoid committing secrets.

## License

MIT License
