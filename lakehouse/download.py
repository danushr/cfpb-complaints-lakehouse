import urllib.request
import zipfile

from lakehouse.paths import RAW_DIR

URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"


def fetch():
    """Download and unzip the CFPB file when its ETag changed. Returns (csv_path, batch_id)."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path, csv_path, etag_file = RAW_DIR / "complaints.csv.zip", RAW_DIR / "complaints.csv", RAW_DIR / "etag.txt"
    with urllib.request.urlopen(urllib.request.Request(URL, method="HEAD")) as r:
        etag, size = r.headers["ETag"].strip('"'), int(r.headers["Content-Length"])
    if not (csv_path.exists() and etag_file.exists() and etag_file.read_text() == etag):
        urllib.request.urlretrieve(URL, zip_path)
        if zip_path.stat().st_size != size:
            raise RuntimeError(f"truncated download: {zip_path.stat().st_size} of {size} bytes")
        with zipfile.ZipFile(zip_path) as z:
            z.extract("complaints.csv", RAW_DIR)
        etag_file.write_text(etag)
    return csv_path, etag[:12]
