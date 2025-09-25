from aria_vrs_extractor.io import Filesystem, join_uri, is_remote


def test_join_uri_local():
    base = "/tmp/root"
    assert join_uri(base, "sensors", "rgb.jsonl") == "/tmp/root/sensors/rgb.jsonl"


def test_join_uri_remote():
    base = "s3://bucket/raw"
    assert join_uri(base, "aria", "rec01") == "s3://bucket/raw/aria/rec01"


def test_filesystem_open_local(tmp_path):
    fs = Filesystem()
    file_path = tmp_path / "example.txt"
    with fs.open(str(file_path), "wt") as handle:
        handle.write("hello")
    assert file_path.read_text() == "hello"
