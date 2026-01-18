import os
import pytest
from astropy.io import fits
from tng_tools.split import download_and_split_hsc_images

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
    download_and_split_hsc_images(
        split_output_dir=str(out_dir),
        parent_output_dir=str(parent_dir),
        URL_LIST=str(url_list),
        BATCH_START=0,
        BATCH_SIZE=1,
        API_KEY="KEY",
        remove_parent=False,
        catalog_path=str(catalog),
        parent_file_only=False
    )

    # Check split file exists
    files = os.listdir(out_dir)
    assert any(f.startswith('50_72_3_') and f.endswith('_hsc_realistic.fits') for f in files)

    # Check catalog contents
    from astropy.table import Table
    tbl = Table.read(str(catalog))
    assert set(tbl.colnames) == {'object_id','filename','filter'}
    assert len(tbl) == len(files)

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
