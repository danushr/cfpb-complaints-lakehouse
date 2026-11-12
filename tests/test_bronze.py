from lakehouse.bronze import load, read_batch

CSV = """Date received,Product,Sub-product,Issue,Sub-issue,Company public response,Company,State,ZIP code,Tags,Submitted via,Date sent to company,Company response to consumer,Timely response?,Complaint ID
2024-07-05,Mortgage,,Issue,,,"ACME, INC.",NY,11211,,Web,2024-07-05,Closed with explanation,Yes,1
"""


def test_load_keeps_raw_text_and_marks_the_batch(spark, tmp_path):
    f = tmp_path / "c.csv"
    f.write_text(CSV)
    path = str(tmp_path / "bronze")
    assert load(spark, str(f), path, "b1") == 1
    r = read_batch(spark, path, "b1").collect()[0]
    assert r["complaint_id"] == "1" and r["company"] == "ACME, INC."
    assert r["_batch_id"] == "b1" and r["_ingested_at"] is not None


def test_loading_the_same_batch_twice_adds_nothing(spark, tmp_path):
    f = tmp_path / "c.csv"
    f.write_text(CSV)
    path = str(tmp_path / "bronze")
    load(spark, str(f), path, "b1")
    assert load(spark, str(f), path, "b1") == 0
    assert read_batch(spark, path).count() == 1


def test_read_batch_without_id_returns_the_newest_batch(spark, tmp_path):
    f = tmp_path / "c.csv"
    f.write_text(CSV)
    path = str(tmp_path / "bronze")
    load(spark, str(f), path, "b1")
    load(spark, str(f), path, "b2")
    assert {r["_batch_id"] for r in read_batch(spark, path).collect()} == {"b2"}
