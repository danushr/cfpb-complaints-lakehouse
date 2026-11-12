from datetime import date

from lakehouse.bronze import RENAME
from lakehouse.silver import clean, quarantine, upsert

COLS = list(RENAME.values())


def raw(spark, *rows):
    return spark.createDataFrame(list(rows), COLS)


def row(cid="1", received="2024-07-05", sent=None, state="NY", timely="Yes", response="Closed with explanation"):
    sent = sent or received
    return (received, "Mortgage", "", "Trouble during payment process", "", "", " ACME BANK ", state,
            "11211", "", "Web", sent, response, timely, cid)


def test_clean_types_trims_and_nulls_empty_strings(spark):
    valid, rejected = clean(raw(spark, row()))
    r = valid.collect()[0]
    assert r.complaint_id == 1
    assert r.date_received == date(2024, 7, 5)
    assert r.company == "ACME BANK"
    assert r.sub_product is None
    assert r.timely_response is True
    assert rejected.count() == 0


def test_rows_failing_a_rule_go_to_quarantine_with_a_reason(spark):
    bad = raw(
        spark,
        row(cid="1"),
        row(cid="abc"),
        row(cid="3", received="not a date"),
        row(cid="4", received="2999-01-01", sent="2999-01-02"),
        row(cid="5", received="2024-07-05", sent="2024-07-01"),
        row(cid="6", timely="Maybe"),
    )
    valid, rejected = clean(bad)
    assert [r.complaint_id for r in valid.collect()] == [1]
    reasons = {r.complaint_id_raw: r.reject_reason for r in rejected.collect()}
    assert reasons == {
        "abc": "complaint_id_not_numeric",
        "3": "date_received_invalid",
        "4": "date_received_in_future",
        "5": "sent_before_received",
        "6": "timely_response_unknown",
    }


def test_duplicate_complaint_ids_in_one_batch_keep_one_row(spark):
    valid, _ = clean(raw(spark, row(cid="7"), row(cid="7")))
    assert valid.count() == 1


def test_upsert_inserts_new_updates_changed_and_ignores_unchanged(spark, tmp_path):
    path = str(tmp_path / "silver")
    first, _ = clean(raw(spark, row(cid="1"), row(cid="2")))
    assert upsert(spark, path, first) == {"inserted": 2, "updated": 0}

    second, _ = clean(raw(spark, row(cid="1"), row(cid="2", response="Closed with monetary relief"), row(cid="3")))
    assert upsert(spark, path, second) == {"inserted": 1, "updated": 1}

    assert upsert(spark, path, second) == {"inserted": 0, "updated": 0}
    out = {r.complaint_id: r.company_response for r in spark.read.format("delta").load(path).collect()}
    assert out == {1: "Closed with explanation", 2: "Closed with monetary relief", 3: "Closed with explanation"}


def test_quarantining_the_same_batch_twice_keeps_one_copy(spark, tmp_path):
    path = str(tmp_path / "quarantine")
    _, rejected = clean(raw(spark, row(cid="abc"), row(cid="9", timely="Maybe")))
    quarantine(spark, path, rejected, "b1")
    quarantine(spark, path, rejected, "b1")
    quarantine(spark, path, rejected, "b2")
    got = spark.read.format("delta").load(path).groupBy("_batch_id").count().collect()
    assert {r["_batch_id"]: r["count"] for r in got} == {"b1": 2, "b2": 2}
