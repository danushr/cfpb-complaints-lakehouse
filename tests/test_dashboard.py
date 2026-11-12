from streamlit.testing.v1 import AppTest

from lakehouse.gold import build
from lakehouse.silver import clean, upsert
from tests.test_silver import raw, row


def test_dashboard_renders_from_gold_tables(spark, tmp_path, monkeypatch):
    silver, gold = str(tmp_path / "silver"), str(tmp_path / "lake" / "gold")
    df, _ = clean(raw(spark, row(cid="1"), row(cid="2", timely="No"), row(cid="3", state="TX")))
    upsert(spark, silver, df)
    build(spark, silver, gold)
    monkeypatch.setenv("LAKE_DATA_DIR", str(tmp_path))

    import importlib
    import lakehouse.paths
    importlib.reload(lakehouse.paths)
    at = AppTest.from_file("../app/dashboard.py", default_timeout=60).run()
    assert not at.exception
    assert at.metric[0].value == "3"
    assert at.metric[2].value == "66.7%"
