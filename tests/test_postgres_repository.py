import importlib


postgres_repository = importlib.import_module("src.ingestion.postgres_repository")
PostgresRepository = postgres_repository.PostgresRepository


class FakeCursor:
    def __init__(self, fetch_rows=None, columns=None, fetch_one_rows=None):
        self.fetch_rows = fetch_rows or []
        self.columns = columns or []
        self.fetch_one_rows = fetch_one_rows or []
        self.executed = []
        self.description = [(name,) for name in self.columns]

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.fetch_rows

    def fetchone(self):
        if self.fetch_one_rows:
            return self.fetch_one_rows.pop(0)
        return None

    def close(self):
        return None


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        return None


class ExplodingCursor(FakeCursor):
    def execute(self, sql, params=None):
        raise RuntimeError("simulated undefined table")


def test_postgres_repository_builds_insert_payload_for_run_history():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    repo.write_run_history(
        {
            "run_id": "run-1",
            "started_at": "2026-02-18T00:00:00+00:00",
            "finished_at": "2026-02-18T00:01:00+00:00",
            "source_name": "sec_edgar",
            "status": "success",
            "raw_written": 10,
            "canonical_written": 8,
            "quarantined": 2,
            "error_message": None,
        }
    )

    assert "INSERT INTO ingestion_runs" in cursor.executed[0][0]
    assert cursor.executed[0][1][0] == "run-1"
    assert conn.committed is True


def test_postgres_repository_reads_latest_runs():
    cursor = FakeCursor(
        fetch_rows=[("run-1", "success", "2026-02-18T00:01:00+00:00")],
        columns=["run_id", "status", "finished_at"],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    rows = repo.read_latest_runs(limit=20)

    assert "ORDER BY finished_at DESC" in cursor.executed[0][0]
    assert rows[0]["run_id"] == "run-1"
    assert rows[0]["status"] == "success"


def test_postgres_repository_writes_pipeline_rows_and_reads_counters():
    cursor = FakeCursor(fetch_one_rows=[(3,), (2,), (1,)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    repo.write_raw({"source": "sec_edgar", "entity_id": "AAPL"})
    repo.write_canonical({"source": "sec_edgar", "entity_id": "AAPL"})
    repo.write_quarantine("quality_gate_failed", {"source": "sec_edgar"})
    counters = repo.read_status_counters()

    assert "INSERT INTO raw_event_store" in cursor.executed[0][0]
    assert "INSERT INTO canonical_fact_store" in cursor.executed[1][0]
    assert "INSERT INTO quarantine_batches" in cursor.executed[2][0]
    assert counters == {"raw_events": 3, "canonical_events": 2, "quarantine_events": 1}


def test_postgres_repository_reads_learning_metrics_for_1m_horizon():
    cursor = FakeCursor(fetch_one_rows=[(20,), (12, 0.5833, 0.0315, -0.0042)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    metrics = repo.read_learning_metrics(horizon="1M")

    forecast_sql, forecast_params = cursor.executed[0]
    assert "FROM forecast_records" in forecast_sql
    assert forecast_params == ("1M",)

    sql, params = cursor.executed[1]
    assert "FROM realization_records rr" in sql
    assert "JOIN forecast_records fr ON fr.id = rr.forecast_id" in sql
    assert params == ("1M",)
    assert metrics["horizon"] == "1M"
    assert metrics["forecast_count"] == 20
    assert metrics["realized_count"] == 12
    assert metrics["realization_coverage"] == 0.6
    assert metrics["hit_rate"] == 0.5833
    assert metrics["mean_abs_forecast_error"] == 0.0315
    assert metrics["mean_signed_forecast_error"] == -0.0042


def test_postgres_repository_writes_investment_thesis_and_returns_id():
    cursor = FakeCursor(fetch_one_rows=[("thesis-1",)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    thesis_id = repo.write_investment_thesis(
        {
            "thesis_id": "thesis-1",
            "created_by": "autopilot",
            "scope_level": "stock",
            "target_id": "AAPL",
            "title": "AI capex cycle persists",
            "summary": "Cloud demand and margins support overweight.",
            "evidence_hard": [{"source": "sec", "metric": "revenue_growth"}],
            "evidence_soft": [{"source": "news", "note": "management tone improved"}],
            "as_of": "2026-02-22T00:00:00+00:00",
            "lineage_id": "lineage-1",
        }
    )

    assert thesis_id == "thesis-1"
    assert "INSERT INTO investment_theses" in cursor.executed[0][0]
    assert "ON CONFLICT (thesis_id) DO UPDATE" in cursor.executed[0][0]
    assert conn.committed is True


def test_postgres_repository_writes_forecast_record_and_returns_id():
    cursor = FakeCursor(fetch_one_rows=[(42, True)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    forecast_id = repo.write_forecast_record(
        {
            "thesis_id": "thesis-1",
            "horizon": "1M",
            "expected_return_low": 0.04,
            "expected_return_high": 0.10,
            "expected_volatility": 0.2,
            "expected_drawdown": 0.12,
            "confidence": 0.7,
            "key_drivers": ["macro:disinflation"],
            "evidence_hard": [{"source": "fred", "metric": "CPI"}],
            "evidence_soft": [{"source": "news", "note": "AI capex sentiment"}],
            "as_of": "2026-02-22T00:00:00+00:00",
        }
    )

    assert forecast_id == 42
    assert "INSERT INTO forecast_records" in cursor.executed[0][0]
    assert "ON CONFLICT (thesis_id, horizon, as_of)" in cursor.executed[0][0]
    assert conn.committed is True


def test_postgres_repository_writes_forecast_record_idempotent_and_flags_dedup():
    cursor = FakeCursor(fetch_one_rows=[(42, False)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    forecast_id, deduplicated = repo.write_forecast_record_idempotent(
        {
            "thesis_id": "thesis-1",
            "horizon": "1M",
            "expected_return_low": 0.04,
            "expected_return_high": 0.10,
            "expected_volatility": 0.2,
            "expected_drawdown": 0.12,
            "confidence": 0.7,
            "key_drivers": ["macro:disinflation"],
            "evidence_hard": [{"source": "fred", "metric": "CPI"}],
            "evidence_soft": [{"source": "news", "note": "AI capex sentiment"}],
            "as_of": "2026-02-22T00:00:00+00:00",
        }
    )

    assert forecast_id == 42
    assert deduplicated is True


def test_postgres_repository_rejects_investment_thesis_without_hard_evidence():
    cursor = FakeCursor(fetch_one_rows=[("thesis-1",)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    try:
        repo.write_investment_thesis(
            {
                "thesis_id": "thesis-1",
                "created_by": "autopilot",
                "scope_level": "stock",
                "target_id": "AAPL",
                "title": "AI capex cycle persists",
                "summary": "Cloud demand and margins support overweight.",
                "evidence_hard": [],
                "evidence_soft": [{"source": "news", "note": "tone improved"}],
                "as_of": "2026-02-22T00:00:00+00:00",
                "lineage_id": "lineage-1",
            }
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "investment thesis requires non-empty evidence_hard" in str(exc)

    assert cursor.executed == []


def test_postgres_repository_rejects_forecast_record_without_hard_evidence():
    cursor = FakeCursor(fetch_one_rows=[(42,)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    try:
        repo.write_forecast_record(
            {
                "thesis_id": "thesis-1",
                "horizon": "1M",
                "expected_return_low": 0.04,
                "expected_return_high": 0.10,
                "expected_volatility": 0.2,
                "expected_drawdown": 0.12,
                "confidence": 0.7,
                "key_drivers": ["macro:disinflation"],
                "evidence_hard": [],
                "evidence_soft": [{"source": "news", "note": "AI capex sentiment"}],
                "as_of": "2026-02-22T00:00:00+00:00",
            }
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "forecast record requires non-empty evidence_hard" in str(exc)

    assert cursor.executed == []


def test_postgres_repository_rejects_investment_thesis_hard_evidence_without_source():
    cursor = FakeCursor(fetch_one_rows=[("thesis-1",)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    try:
        repo.write_investment_thesis(
            {
                "thesis_id": "thesis-1",
                "created_by": "autopilot",
                "scope_level": "stock",
                "target_id": "AAPL",
                "title": "AI capex cycle persists",
                "summary": "Cloud demand and margins support overweight.",
                "evidence_hard": [{"metric": "revenue_growth"}],
                "evidence_soft": [{"source": "news", "note": "tone improved"}],
                "as_of": "2026-02-22T00:00:00+00:00",
                "lineage_id": "lineage-1",
            }
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "investment thesis evidence_hard[0] requires non-empty source" in str(exc)

    assert cursor.executed == []


def test_postgres_repository_rejects_forecast_record_hard_evidence_non_object():
    cursor = FakeCursor(fetch_one_rows=[(42,)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    try:
        repo.write_forecast_record(
            {
                "thesis_id": "thesis-1",
                "horizon": "1M",
                "expected_return_low": 0.04,
                "expected_return_high": 0.10,
                "expected_volatility": 0.2,
                "expected_drawdown": 0.12,
                "confidence": 0.7,
                "key_drivers": ["macro:disinflation"],
                "evidence_hard": ["fred:CPI"],
                "evidence_soft": [{"source": "news", "note": "AI capex sentiment"}],
                "as_of": "2026-02-22T00:00:00+00:00",
            }
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "forecast record evidence_hard[0] must be an object" in str(exc)

    assert cursor.executed == []


def test_postgres_repository_computes_realization_hit_and_forecast_error_from_forecast_range():
    cursor = FakeCursor(fetch_one_rows=[(0.02, 0.08), (99,)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    realization_id = repo.write_realization_from_outcome(
        forecast_id=7,
        realized_return=0.05,
        evaluated_at="2026-03-22T00:00:00+00:00",
        realized_volatility=0.18,
        max_drawdown=0.09,
    )

    assert realization_id == 99
    assert "SELECT expected_return_low, expected_return_high" in cursor.executed[0][0]
    insert_sql, insert_params = cursor.executed[1]
    assert "INSERT INTO realization_records" in insert_sql
    # midpoint(0.02, 0.08)=0.05, so forecast_error should be 0.0 and hit=True
    assert insert_params[4] is True
    assert insert_params[5] == 0.0


def test_postgres_repository_writes_forecast_error_attribution_and_returns_id():
    cursor = FakeCursor(fetch_one_rows=[(321,)])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    attribution_id = repo.write_forecast_error_attribution(
        {
            "realization_id": 99,
            "category": "macro_miss",
            "contribution": -0.03,
            "note": "inflation re-acceleration",
            "evidence_hard": [{"source": "fred", "metric": "CPI"}],
            "evidence_soft": [{"source": "analyst", "note": "policy surprise"}],
        }
    )

    assert attribution_id == 321
    assert "INSERT INTO forecast_error_attributions" in cursor.executed[0][0]
    assert conn.committed is True


def test_postgres_repository_raises_when_forecast_missing_for_realization_write():
    cursor = FakeCursor(fetch_one_rows=[None])
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    try:
        repo.write_realization_from_outcome(
            forecast_id=123,
            realized_return=0.01,
            evaluated_at="2026-03-22T00:00:00+00:00",
        )
    except ValueError as exc:
        assert "forecast_id not found" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing forecast_id")


def test_postgres_repository_reads_forecast_error_attributions_with_hard_soft_evidence():
    cursor = FakeCursor(
        fetch_rows=[
            (
                501,
                99,
                "macro_miss",
                -0.03,
                "inflation re-acceleration",
                [{"source": "fred", "metric": "CPI"}],
                [{"source": "analyst", "note": "policy surprise"}],
                "2026-03-22T00:05:00+00:00",
                7,
                0.01,
                -0.04,
                False,
                "2026-03-22T00:00:00+00:00",
                "1M",
                "thesis-1",
                "2026-02-22T00:00:00+00:00",
                0.02,
                0.08,
                "stock",
                "AAPL",
                "AI capex cycle persists",
            )
        ],
        columns=[
            "attribution_id",
            "realization_id",
            "category",
            "contribution",
            "note",
            "evidence_hard",
            "evidence_soft",
            "created_at",
            "forecast_id",
            "realized_return",
            "forecast_error",
            "hit",
            "evaluated_at",
            "horizon",
            "thesis_id",
            "as_of",
            "expected_return_low",
            "expected_return_high",
            "scope_level",
            "target_id",
            "title",
        ],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    rows = repo.read_forecast_error_attributions(horizon="1M", limit=30)

    sql, params = cursor.executed[0]
    assert "FROM forecast_error_attributions fea" in sql
    assert "JOIN realization_records rr ON rr.id = fea.realization_id" in sql
    assert params == ("1M", 30)
    assert rows[0]["attribution_id"] == 501
    assert rows[0]["category"] == "macro_miss"
    assert rows[0]["evidence_hard"][0]["source"] == "fred"
    assert rows[0]["evidence_soft"][0]["source"] == "analyst"


def test_postgres_repository_reads_forecast_error_attribution_detail_payload():
    cursor = FakeCursor(
        fetch_one_rows=[
            (
                501,
                7,
                "thesis-1",
                "2026-02-22T00:00:00+00:00",
                "2026-03-22T00:00:00+00:00",
                "2026-03-22T00:05:00+00:00",
                [{"source": "fred", "metric": "CPI", "lineage_id": "lin-1"}],
                [{"source": "analyst", "note": "policy surprise"}],
            )
        ],
        columns=[
            "attribution_id",
            "forecast_id",
            "thesis_id",
            "as_of",
            "evaluated_at",
            "created_at",
            "evidence_hard",
            "evidence_soft",
        ],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    payload = repo.read_forecast_error_attribution_detail(attribution_id=501, max_preview_chars=80)

    assert payload is not None
    sql, params = cursor.executed[0]
    assert "FROM forecast_error_attributions fea" in sql
    assert params == (501,)
    assert payload["attribution_id"] == 501
    assert payload["hard_evidence_refs"][0]["source"] == "fred"
    assert payload["lineage_summary"] == ["lin-1"]


def test_postgres_repository_reads_expected_vs_realized_with_evidence_fields():
    cursor = FakeCursor(
        fetch_rows=[
            (
                7,
                "1M",
                "2026-02-22T00:00:00+00:00",
                0.02,
                0.08,
                0.2,
                0.12,
                0.7,
                [{"driver": "macro:disinflation"}],
                [{"source": "fred", "metric": "CPI"}],
                [{"source": "news", "note": "tone improved"}],
                99,
                0.05,
                0.18,
                0.09,
                True,
                0.0,
                "2026-03-22T00:00:00+00:00",
                "thesis-1",
                "stock",
                "AAPL",
                "AI capex cycle persists",
                "Cloud demand supports overweight.",
                [{"source": "sec", "metric": "revenue_growth"}],
                [{"source": "news", "note": "management tone improved"}],
            )
        ],
        columns=[
            "forecast_id",
            "horizon",
            "as_of",
            "expected_return_low",
            "expected_return_high",
            "expected_volatility",
            "expected_drawdown",
            "confidence",
            "key_drivers",
            "forecast_evidence_hard",
            "forecast_evidence_soft",
            "realization_id",
            "realized_return",
            "realized_volatility",
            "max_drawdown",
            "hit",
            "forecast_error",
            "evaluated_at",
            "thesis_id",
            "scope_level",
            "target_id",
            "title",
            "summary",
            "thesis_evidence_hard",
            "thesis_evidence_soft",
        ],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    rows = repo.read_expected_vs_realized(horizon="1M", limit=25)

    sql, params = cursor.executed[0]
    assert "FROM forecast_records fr" in sql
    assert "LEFT JOIN realization_records rr ON rr.forecast_id = fr.id" in sql
    assert params == ("1M", 25)
    assert rows[0]["forecast_id"] == 7
    assert rows[0]["realization_id"] == 99
    assert rows[0]["forecast_evidence_hard"][0]["source"] == "fred"
    assert rows[0]["thesis_evidence_soft"][0]["source"] == "news"


def test_postgres_repository_reads_forecast_error_category_stats():
    cursor = FakeCursor(
        fetch_rows=[
            ("macro_miss", 4, -0.015, 0.018),
            ("valuation_miss", 2, -0.006, 0.006),
        ],
        columns=[
            "category",
            "attribution_count",
            "mean_contribution",
            "mean_abs_contribution",
        ],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    rows = repo.read_forecast_error_category_stats(horizon="1M", limit=5)

    sql, params = cursor.executed[0]
    assert "FROM forecast_error_attributions fea" in sql
    assert "GROUP BY fea.category" in sql
    assert params == ("1M", 5)
    assert rows[0]["category"] == "macro_miss"
    assert rows[0]["attribution_count"] == 4


def test_postgres_repository_learning_reads_fallback_when_tables_missing():
    cursor = ExplodingCursor()
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    assert repo.read_latest_runs(limit=5) == []
    assert repo.read_status_counters() == {
        "raw_events": 0,
        "canonical_events": 0,
        "quarantine_events": 0,
    }

    metrics = repo.read_learning_metrics(horizon="1M")
    assert metrics["horizon"] == "1M"
    assert metrics["forecast_count"] == 0
    assert metrics["realized_count"] == 0
    assert metrics["realization_coverage"] is None

    assert repo.read_forecast_error_attributions(horizon="1M", limit=10) == []
    assert repo.read_expected_vs_realized(horizon="1M", limit=10) == []
    assert repo.read_forecast_error_category_stats(horizon="1M", limit=10) == []


def test_postgres_repository_create_refresh_request_handles_dedup_and_returns_row():
    cursor = FakeCursor(
        fetch_one_rows=[
            (
                101,
                "2026-02-23T02:30:00+00:00",
                "macro_signal",
                "enduser/signals",
                "pending",
                "enduser_session",
                None,
                None,
                None,
                None,
                None,
                False,
            )
        ],
        columns=[
            "id",
            "requested_at",
            "request_type",
            "source_view",
            "status",
            "requested_by",
            "note",
            "handled_at",
            "handler",
            "result_message",
            "ingestion_run_id",
            "deduplicated",
        ],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    row = repo.create_refresh_request(
        request_type="macro_signal",
        source_view="enduser/signals",
        requested_by="enduser_session",
        cooldown_minutes=10,
    )

    assert row["id"] == 101
    assert row["deduplicated"] is False
    assert "WITH recent_pending AS" in cursor.executed[0][0]
    assert conn.committed is True


def test_postgres_repository_reads_pending_refresh_requests():
    cursor = FakeCursor(
        fetch_rows=[
            (
                102,
                "2026-02-23T02:31:00+00:00",
                "macro_signal",
                "enduser/signals",
                "pending",
                "enduser_session",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        columns=[
            "id",
            "requested_at",
            "request_type",
            "source_view",
            "status",
            "requested_by",
            "note",
            "handled_at",
            "handler",
            "result_message",
            "ingestion_run_id",
        ],
    )
    conn = FakeConnection(cursor)
    repo = PostgresRepository(connection_factory=lambda: conn)

    rows = repo.read_pending_refresh_requests(limit=15)

    assert rows[0]["id"] == 102
    assert rows[0]["status"] == "pending"
    assert "FROM refresh_requests" in cursor.executed[0][0]
