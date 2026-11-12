from lakehouse.gold import build
from lakehouse.silver import clean, upsert
from tests.test_silver import raw, row


def test_gold_tables_aggregate_silver(spark, tmp_path):
    silver, gold = str(tmp_path / "silver"), str(tmp_path / "gold")
    df, _ = clean(raw(
        spark,
        row(cid="1", received="2024-07-05", timely="Yes", response="Closed with explanation"),
        row(cid="2", received="2024-07-20", timely="No", response="Closed with monetary relief"),
        row(cid="3", received="2024-08-01", timely="Yes", response="Closed with non-monetary relief"),
        row(cid="4", received="2024-08-02", state="TX", timely="Yes", response="Closed with explanation"),
    ))
    upsert(spark, silver, df)
    build(spark, silver, gold)

    by_month = {(r.month.isoformat(), r.product): r.complaints for r in spark.read.format("delta").load(f"{gold}/complaints_by_month").collect()}
    assert by_month == {("2024-07-01", "Mortgage"): 2, ("2024-08-01", "Mortgage"): 2}

    card = spark.read.format("delta").load(f"{gold}/company_scorecard").collect()[0]
    assert (card.company, card.complaints) == ("ACME BANK", 4)
    assert card.timely_rate == 0.75
    assert card.relief_rate == 0.5

    states = {(r.state, r.year): r.complaints for r in spark.read.format("delta").load(f"{gold}/state_year").collect()}
    assert states == {("NY", 2024): 3, ("TX", 2024): 1}
