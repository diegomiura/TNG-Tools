import os
import pytest
from astropy.io import fits
from tng_tools.split import (
    CATALOG_COLUMN_ORDER,
    build_catalog_from_split_images,
    download_and_split_hsc_images,
)

class DummyResponse:
    def __init__(self, content):
        self._content = content
    def raise_for_status(self):
        pass
    def iter_content(self, chunk_size):
        yield self._content

def create_test_fits(tmp_path):
    hdu0 = fits.PrimaryHDU()
    hdu1 = fits.ImageHDU(data=[[1,2],[3,4]])
    hdu1.header['EXTNAME'] = 'SUBARU_HSC.G'
    hdul = fits.HDUList([hdu0, hdu1])
    parent = tmp_path / 'test_data.fits'
    hdul.writeto(parent)
    return parent


def create_merger_csv(tmp_path, sim='50'):
    merger_dir = tmp_path / 'merger_history'
    merger_dir.mkdir()
    csv_path = merger_dir / f'Mergers_TNG{sim}-1.csv'
    csv_path.write_text(
        'dbID,Major_CountSince1Gyr,Major_CountUntil1Gyr,Major_TimeSinceMerger,Major_TimeUntilMerger,'
        'Minor_CountSince1Gyr,Minor_CountUntil1Gyr,Minor_TimeSinceMerger,Minor_TimeUntilMerger,'
        'Mini_CountSince1Gyr,Mini_CountUntil1Gyr,Mini_TimeSinceMerger,Mini_TimeUntilMerger\n'
        '72_3,2,1,0.42,0.13,0,3,-1.0,0.5,1,0,0.2,-1.0\n'
    )
    return merger_dir

def test_split_functionality(tmp_path, monkeypatch):
    # Create fake FITS file and URL list
    parent = create_test_fits(tmp_path)
    url = f"http://example.com/api/TNG50-1/snapshots/72/subhalos/3/skirt/{parent.name}"
    url_list = tmp_path / 'urls.txt'
    url_list.write_text(url + '\n')

    # Monkeypatch requests.get to return our FITS data
    def fake_get(u, headers=None, stream=False):
        assert u == url
        content = parent.read_bytes()
        return DummyResponse(content)
    monkeypatch.setattr('requests.get', fake_get)

    # Run split
    out_dir = tmp_path / 'out'
    parent_dir = tmp_path / 'parents'
    catalog = tmp_path / 'catalog.fits'
    empty_merger_dir = tmp_path / 'empty_merger_dir'
    empty_merger_dir.mkdir()
    download_and_split_hsc_images(
        split_output_dir=str(out_dir),
        parent_output_dir=str(parent_dir),
        URL_LIST=str(url_list),
        BATCH_START=0,
        BATCH_SIZE=1,
        API_KEY="KEY",
        remove_parent=False,
        catalog_path=str(catalog),
        parent_file_only=False,
        merger_history_dir=str(empty_merger_dir),
    )

    # Check split file exists
    files = os.listdir(out_dir)
    assert any(f.startswith('50_72_3_') and f.endswith('_hsc_realistic.fits') for f in files)

    # Check catalog contents
    from astropy.table import Table
    tbl = Table.read(str(catalog))
    assert set(tbl.colnames) == set(CATALOG_COLUMN_ORDER)
    assert len(tbl) == len(files)
    assert set(tbl['has_merger_row']) == {False}


def test_split_adds_merger_labels(tmp_path, monkeypatch):
    parent = create_test_fits(tmp_path)
    url = f"http://example.com/api/TNG50-1/snapshots/72/subhalos/3/skirt/{parent.name}"
    url_list = tmp_path / 'urls.txt'
    url_list.write_text(url + '\n')
    merger_dir = create_merger_csv(tmp_path, sim='50')

    def fake_get(u, headers=None, stream=False):
        assert u == url
        content = parent.read_bytes()
        return DummyResponse(content)
    monkeypatch.setattr('requests.get', fake_get)

    out_dir = tmp_path / 'out'
    catalog = tmp_path / 'catalog.fits'
    download_and_split_hsc_images(
        split_output_dir=str(out_dir),
        URL_LIST=str(url_list),
        BATCH_START=0,
        BATCH_SIZE=1,
        API_KEY="KEY",
        remove_parent=True,
        catalog_path=str(catalog),
        parent_file_only=False,
        merger_history_dir=str(merger_dir),
    )

    from astropy.table import Table
    tbl = Table.read(str(catalog))
    assert len(tbl) == 1
    row = tbl[0]
    assert row['dbid'] == '72_3'
    assert row['sim'] == 50
    assert row['snapshot'] == 72
    assert row['subhalo'] == 3
    assert row['has_merger_row']
    assert row['has_major_past_1gyr']
    assert row['has_major_future_1gyr']
    assert not row['has_minor_past_1gyr']
    assert row['has_minor_future_1gyr']
    assert row['has_mini_past_1gyr']
    assert not row['has_mini_future_1gyr']
    assert row['major_count_since_1gyr'] == 2
    assert row['minor_count_until_1gyr'] == 3
    assert row['mini_time_since_merger'] == pytest.approx(0.2)

def test_parent_only(tmp_path, monkeypatch):
    parent = create_test_fits(tmp_path)
    url = f"http://example.com/api/TNG50-1/snapshots/72/subhalos/3/skirt/{parent.name}"
    url_list = tmp_path / 'urls.txt'
    url_list.write_text(url + '\n')

    def fake_get(u, headers=None, stream=False):
        content = parent.read_bytes()
        return DummyResponse(content)
    monkeypatch.setattr('requests.get', fake_get)

    out_dir = tmp_path / 'out2'
    parent_dir = tmp_path / 'parents'
    download_and_split_hsc_images(
        split_output_dir=str(out_dir),
        parent_output_dir=str(parent_dir),
        URL_LIST=str(url_list),
        BATCH_START=0,
        BATCH_SIZE=1,
        API_KEY="KEY",
        remove_parent=False,
        catalog_path=None,
        parent_file_only=True
    )

    expected_parent = parent_dir / '50_72_3_v?_parent.fits'
    assert parent_dir.exists()
    assert expected_parent.exists()
    # No split outputs
    assert not out_dir.exists()


def test_catalog_command_rebuilds_from_existing_splits(tmp_path):
    split_dir = tmp_path / 'split_images'
    split_dir.mkdir()
    (split_dir / '50_72_3_G_v2_hsc_realistic.fits').touch()
    (split_dir / '50_72_3_R_v2_hsc_realistic.fits').touch()
    (split_dir / 'ignore_me.fits').touch()

    merger_dir = create_merger_csv(tmp_path, sim='50')
    catalog = tmp_path / 'catalog.fits'

    build_catalog_from_split_images(
        split_output_dir=str(split_dir),
        catalog_path=str(catalog),
        catalog_append=False,
        merger_history_dir=str(merger_dir),
        recursive=False,
    )

    from astropy.table import Table
    tbl = Table.read(str(catalog))
    assert set(tbl.colnames) == set(CATALOG_COLUMN_ORDER)
    assert len(tbl) == 2
    assert set(tbl['filter']) == {'G', 'R'}
    assert set(tbl['has_merger_row']) == {True}
    assert set(tbl['dbid']) == {'72_3'}
