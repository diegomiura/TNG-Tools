import os
import pytest
from tng_tools.fetch import make_list_of_urls

class DummyResponse:
    def __init__(self, json_data):
        self._json = json_data
    def raise_for_status(self):
        pass
    def json(self):
        return self._json

def test_make_list_of_urls(tmp_path, monkeypatch):
    # Prepare fake API responses
    snapshot_urls = [
        "http://example.com/files/skirt_images_hsc_realistic_v2_72"
    ]
    file_listing = {"files": ["file1.fits", "file2.fits"]}
    # Monkeypatch requests.get to return fake JSON
    def fake_get(url, headers=None):
        if url.endswith('/skirt_images_hsc/'):
            return DummyResponse(snapshot_urls)
        elif url == snapshot_urls[0]:
            return DummyResponse(file_listing)
        else:
            pytest.skip(f"Unexpected URL: {url}")
    monkeypatch.setattr('requests.get', fake_get)

    # Run in temporary directory
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        make_list_of_urls(API_KEY="KEY",
                          BASE_API_URL="http://example.com",
                          ENDING="/files/skirt_images_hsc/",
                          SNAPSHOT_FILTER="realistic")
        out = tmp_path / 'all_file_urls.txt'
        assert out.exists()
        lines = out.read_text().splitlines()
        assert lines == ["file1.fits", "file2.fits"]
    finally:
        os.chdir(cwd)
