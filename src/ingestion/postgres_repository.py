import json
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from uuid import uuid4
from typing import Optional, Protocol, cast

import psycopg2

from src.research.contracts import NormalizedSeriesPoint


class CursorProtocol(Protocol):
    description: list[tuple[str]]

    def execute(self, sql: str, params: tuple[object, ...]) -> None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchone(self) -> Optional[tuple[object, ...]]: ...

    def close(self) -> None: ...


class ConnectionProtocol(Protocol):
    def cursor(self) -> CursorProtocol: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


class PostgresRepository:
    def __init__(
        self,
        dsn: str = "",
        connection_factory: Optional[Callable[[], ConnectionProtocol]] = None,
    ) -> None:
        self._dsn: str = dsn
        self._connection_factory: Optional[Callable[[], ConnectionProtocol]] = (
            connection_factory
        )

    @staticmethod
    def _require_hard_evidence(
        evidence_hard: object,
        context: str,
    ) -> None:
        if not isinstance(evidence_hard, list) or len(evidence_hard) == 0:
            raise ValueError(
                f"{context} requires non-empty evidence_hard (HARD evidence)"
            )

        for index, item in enumerate(evidence_hard):
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"{context} evidence_hard[{index}] must be an object with traceable metadata"
                )

            source = item.get("source")
            if not isinstance(source, str) or source.strip() == "":
                raise ValueError(
                    f"{context} evidence_hard[{index}] requires non-empty source"
                )

    def _connect(self) -> ConnectionProtocol:
        if self._connection_factory is not None:
            return self._connection_factory()
        if not self._dsn:
            raise ValueError("dsn is required when no connection_factory is provided")
        return cast(
            ConnectionProtocol,
            cast(object, psycopg2.connect(self._dsn)),
        )

    def write_run_history(self, run: Mapping[str, object]) -> None:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ingestion_runs(
                run_id,
                started_at,
                finished_at,
                source_name,
                status,
                raw_written,
                canonical_written,
                quarantined,
                error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run["run_id"],
                run["started_at"],
                run["finished_at"],
                run["source_name"],
                run["status"],
                run["raw_written"],
                run["canonical_written"],
                run["quarantined"],
                run["error_message"],
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def write_macro_analysis_result(self, result: Mapping[str, object]) -> None:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            INSERT INTO macro_analysis_results(
                run_id,
                as_of,
                regime,
                confidence,
                base_case,
                bull_case,
                bear_case,
                policy_case,
                critic_case,
                reason_codes,
                risk_flags,
                triggers,
                evidence_hard,
                evidence_soft,
                narrative,
                model
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s)
            """,
            (
                result["run_id"],
                result["as_of"],
                result["regime"],
                result["confidence"],
                result["base_case"],
                result["bull_case"],
                result["bear_case"],
                result.get("policy_case", ""),
                result.get("critic_case", ""),
                json.dumps(result["reason_codes"], default=str),
                json.dumps(result["risk_flags"], default=str),
                json.dumps(result["triggers"], default=str),
                json.dumps(result.get("evidence_hard", []), default=str),
                json.dumps(result.get("evidence_soft", []), default=str),
                result["narrative"],
                result.get("model"),
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def write_stock_analysis_result(self, result: Mapping[str, object]) -> None:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            INSERT INTO stock_analysis_results(
                run_id,
                ticker,
                company_name,
                market,
                as_of,
                bull_case,
                bear_case,
                fundamental_case,
                value_case,
                growth_case,
                risk_case,
                critic_case,
                narrative,
                model
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                result["run_id"],
                result["ticker"],
                result.get("company_name"),
                result.get("market"),
                result["as_of"],
                result.get("bull_case", ""),
                result.get("bear_case", ""),
                result.get("fundamental_case", ""),
                result.get("value_case", ""),
                result.get("growth_case", ""),
                result.get("risk_case", ""),
                result.get("critic_case", ""),
                result.get("narrative", ""),
                result.get("model"),
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def read_latest_stock_analysis(
        self, ticker: str = "", limit: int = 20
    ) -> list[dict[str, object]]:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        if ticker:
            cursor.execute(
                """
                SELECT
                    run_id, ticker, company_name, market, as_of,
                    bull_case, bear_case, fundamental_case, value_case,
                    growth_case, risk_case, critic_case, narrative, model, created_at
                FROM stock_analysis_results
                WHERE ticker = %s
                ORDER BY as_of DESC, created_at DESC
                LIMIT %s
                """,
                (ticker, limit),
            )
        else:
            cursor.execute(
                """
                SELECT
                    run_id, ticker, company_name, market, as_of,
                    bull_case, bear_case, fundamental_case, value_case,
                    growth_case, risk_case, critic_case, narrative, model, created_at
                FROM stock_analysis_results
                ORDER BY as_of DESC, created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def read_latest_macro_analysis(self, limit: int = 20) -> list[dict[str, object]]:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            SELECT
                run_id,
                as_of,
                regime,
                confidence,
                base_case,
                bull_case,
                bear_case,
                policy_case,
                critic_case,
                reason_codes,
                risk_flags,
                triggers,
                evidence_hard,
                evidence_soft,
                narrative,
                model,
                created_at
            FROM macro_analysis_results
            ORDER BY as_of DESC, created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def write_investment_thesis(self, thesis: Mapping[str, object]) -> str:
        self._require_hard_evidence(
            thesis.get("evidence_hard"),
            "investment thesis",
        )
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            INSERT INTO investment_theses(
                thesis_id,
                created_by,
                scope_level,
                target_id,
                title,
                summary,
                evidence_hard,
                evidence_soft,
                as_of,
                lineage_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
            ON CONFLICT (thesis_id) DO UPDATE SET
                created_by = EXCLUDED.created_by,
                scope_level = EXCLUDED.scope_level,
                target_id = EXCLUDED.target_id,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                evidence_hard = EXCLUDED.evidence_hard,
                evidence_soft = EXCLUDED.evidence_soft,
                as_of = EXCLUDED.as_of,
                lineage_id = EXCLUDED.lineage_id
            RETURNING thesis_id
            """,
            (
                thesis["thesis_id"],
                thesis.get("created_by", "system"),
                thesis["scope_level"],
                thesis["target_id"],
                thesis["title"],
                thesis["summary"],
                json.dumps(thesis.get("evidence_hard", []), default=str),
                json.dumps(thesis.get("evidence_soft", []), default=str),
                thesis["as_of"],
                thesis["lineage_id"],
            ),
        )
        row = cursor.fetchone() or (thesis["thesis_id"],)
        conn.commit()
        cursor.close()
        conn.close()
        return str(row[0])

    def write_forecast_record(self, record: Mapping[str, object]) -> int:
        forecast_id, _ = self.write_forecast_record_idempotent(record)
        return forecast_id

    def write_forecast_record_idempotent(self, record: Mapping[str, object]) -> tuple[int, bool]:
        self._require_hard_evidence(
            record.get("evidence_hard"),
            "forecast record",
        )
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            INSERT INTO forecast_records(
                thesis_id,
                horizon,
                expected_return_low,
                expected_return_high,
                expected_volatility,
                expected_drawdown,
                confidence,
                key_drivers,
                evidence_hard,
                evidence_soft,
                as_of
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (thesis_id, horizon, as_of) DO UPDATE SET
                expected_return_low = EXCLUDED.expected_return_low,
                expected_return_high = EXCLUDED.expected_return_high,
                expected_volatility = EXCLUDED.expected_volatility,
                expected_drawdown = EXCLUDED.expected_drawdown,
                confidence = EXCLUDED.confidence,
                key_drivers = EXCLUDED.key_drivers,
                evidence_hard = EXCLUDED.evidence_hard,
                evidence_soft = EXCLUDED.evidence_soft
            RETURNING id, (xmax = 0) AS inserted
            """,
            (
                record["thesis_id"],
                record["horizon"],
                record["expected_return_low"],
                record["expected_return_high"],
                record.get("expected_volatility"),
                record.get("expected_drawdown"),
                record["confidence"],
                json.dumps(record.get("key_drivers", []), default=str),
                json.dumps(record.get("evidence_hard", []), default=str),
                json.dumps(record.get("evidence_soft", []), default=str),
                record["as_of"],
            ),
        )
        row = cursor.fetchone() or (0, False)
        conn.commit()
        cursor.close()
        conn.close()
        return int(row[0]), not bool(row[1])

    def write_realization_from_outcome(
        self,
        forecast_id: int,
        realized_return: float,
        evaluated_at: object,
        realized_volatility: Optional[float] = None,
        max_drawdown: Optional[float] = None,
    ) -> int:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        cursor.execute(
            """
            SELECT expected_return_low, expected_return_high
            FROM forecast_records
            WHERE id = %s
            """,
            (forecast_id,),
        )
        forecast_row = cursor.fetchone()
        if forecast_row is None:
            cursor.close()
            conn.close()
            raise ValueError(f"forecast_id not found: {forecast_id}")

        expected_low = float(forecast_row[0])
        expected_high = float(forecast_row[1])
        expected_mid = (expected_low + expected_high) / 2
        forecast_error = expected_mid - realized_return
        hit = expected_low <= realized_return <= expected_high

        cursor.execute(
            """
            INSERT INTO realization_records(
                forecast_id,
                realized_return,
                realized_volatility,
                max_drawdown,
                hit,
                forecast_error,
                evaluated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                forecast_id,
                realized_return,
                realized_volatility,
                max_drawdown,
                hit,
                forecast_error,
                evaluated_at,
            ),
        )
        row = cursor.fetchone() or (0,)
        conn.commit()
        cursor.close()
        conn.close()
        return int(row[0])

    def write_forecast_error_attribution(self, attribution: Mapping[str, object]) -> int:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            INSERT INTO forecast_error_attributions(
                realization_id,
                category,
                contribution,
                note,
                evidence_hard,
                evidence_soft
            ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
            RETURNING id
            """,
            (
                attribution["realization_id"],
                attribution["category"],
                attribution.get("contribution"),
                attribution.get("note"),
                json.dumps(attribution.get("evidence_hard", []), default=str),
                json.dumps(attribution.get("evidence_soft", []), default=str),
            ),
        )
        row = cursor.fetchone() or (0,)
        conn.commit()
        cursor.close()
        conn.close()
        return int(row[0])

    def read_forecast_error_attributions(
        self,
        horizon: str = "1M",
        limit: int = 100,
    ) -> list[dict[str, object]]:
        """Read forecast-error attribution rows for learning-loop diagnostics.

        Keeps HARD/SOFT evidence separated for traceable post-mortem analysis.
        Missing learning-loop tables must not crash operator dashboards.
        """
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    fea.id AS attribution_id,
                    fea.realization_id,
                    fea.category,
                    fea.contribution,
                    fea.note,
                    fea.evidence_hard,
                    fea.evidence_soft,
                    fea.created_at,
                    rr.forecast_id,
                    rr.realized_return,
                    rr.forecast_error,
                    rr.hit,
                    rr.evaluated_at,
                    fr.horizon,
                    fr.thesis_id,
                    fr.as_of,
                    fr.expected_return_low,
                    fr.expected_return_high,
                    it.scope_level,
                    it.target_id,
                    it.title
                FROM forecast_error_attributions fea
                JOIN realization_records rr ON rr.id = fea.realization_id
                JOIN forecast_records fr ON fr.id = rr.forecast_id
                JOIN investment_theses it ON it.thesis_id = fr.thesis_id
                WHERE fr.horizon = %s
                ORDER BY rr.evaluated_at DESC, fea.id DESC
                LIMIT %s
                """,
                (horizon, limit),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    def read_expected_vs_realized(
        self,
        horizon: str = "1M",
        limit: int = 100,
    ) -> list[dict[str, object]]:
        """Read forecast-vs-realization rows for learning loop evaluation.

        Returns thesis metadata and HARD/SOFT evidence columns for traceability.
        Missing learning-loop tables must not crash consumers.
        """
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    fr.id AS forecast_id,
                    fr.horizon,
                    fr.as_of,
                    fr.expected_return_low,
                    fr.expected_return_high,
                    fr.expected_volatility,
                    fr.expected_drawdown,
                    fr.confidence,
                    fr.key_drivers,
                    fr.evidence_hard AS forecast_evidence_hard,
                    fr.evidence_soft AS forecast_evidence_soft,
                    rr.id AS realization_id,
                    rr.realized_return,
                    rr.realized_volatility,
                    rr.max_drawdown,
                    rr.hit,
                    rr.forecast_error,
                    rr.evaluated_at,
                    it.thesis_id,
                    it.scope_level,
                    it.target_id,
                    it.title,
                    it.summary,
                    it.evidence_hard AS thesis_evidence_hard,
                    it.evidence_soft AS thesis_evidence_soft
                FROM forecast_records fr
                JOIN investment_theses it ON it.thesis_id = fr.thesis_id
                LEFT JOIN realization_records rr ON rr.forecast_id = fr.id
                WHERE fr.horizon = %s
                ORDER BY fr.as_of DESC, fr.id DESC
                LIMIT %s
                """,
                (horizon, limit),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    def read_forecast_error_category_stats(
        self,
        horizon: str = "1M",
        limit: int = 20,
    ) -> list[dict[str, object]]:
        """Aggregate attribution categories for forecast-error learning automation.

        Missing learning-loop tables must not crash operator dashboards.
        """
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    fea.category,
                    COUNT(*) AS attribution_count,
                    AVG(fea.contribution) AS mean_contribution,
                    AVG(ABS(fea.contribution)) AS mean_abs_contribution
                FROM forecast_error_attributions fea
                JOIN realization_records rr ON rr.id = fea.realization_id
                JOIN forecast_records fr ON fr.id = rr.forecast_id
                WHERE fr.horizon = %s
                GROUP BY fea.category
                ORDER BY attribution_count DESC, fea.category ASC
                LIMIT %s
                """,
                (horizon, limit),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    def write_raw(self, row: Mapping[str, object]) -> None:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        now = datetime.now(timezone.utc)
        cursor.execute(
            """
            INSERT INTO raw_event_store(
                source,
                entity_id,
                as_of,
                available_at,
                ingested_at,
                lineage_id,
                schema_version,
                license_tier,
                payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(row.get("source", "unknown")),
                str(row.get("entity_id", "unknown")),
                now,
                now,
                now,
                str(row.get("lineage_id", uuid4())),
                str(row.get("schema_version", "v1")),
                str(row.get("license_tier", "gold")),
                json.dumps(dict(row), default=str),
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def write_canonical(self, row: Mapping[str, object]) -> None:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        now = datetime.now(timezone.utc)
        cursor.execute(
            """
            INSERT INTO canonical_fact_store(
                source,
                entity_id,
                as_of,
                available_at,
                ingested_at,
                license_tier,
                lineage_id,
                metric_name,
                metric_value,
                schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(row.get("source", "unknown")),
                str(row.get("entity_id", "unknown")),
                now,
                now,
                now,
                str(row.get("license_tier", "gold")),
                str(row.get("lineage_id", uuid4())),
                "pipeline_events",
                1,
                str(row.get("schema_version", "v1")),
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def write_quarantine(self, reason: str, payload: Mapping[str, object]) -> None:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        cursor.execute(
            """
            INSERT INTO quarantine_batches(batch_id, reason, payload)
            VALUES (%s, %s, %s::jsonb)
            """,
            (str(uuid4()), reason, json.dumps(dict(payload), default=str)),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def read_latest_runs(self, limit: int = 20) -> list[dict[str, object]]:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT run_id, status, finished_at
                FROM ingestion_runs
                ORDER BY finished_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    def read_status_counters(self) -> dict[str, int]:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        def to_int(value: object) -> int:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            if isinstance(value, str):
                return int(value)
            return 0

        try:
            cursor.execute("SELECT COUNT(*) FROM raw_event_store", ())
            raw_row = cursor.fetchone() or (0,)

            cursor.execute("SELECT COUNT(*) FROM canonical_fact_store", ())
            canonical_row = cursor.fetchone() or (0,)

            cursor.execute("SELECT COUNT(*) FROM quarantine_batches", ())
            quarantine_row = cursor.fetchone() or (0,)

            return {
                "raw_events": to_int(raw_row[0]),
                "canonical_events": to_int(canonical_row[0]),
                "quarantine_events": to_int(quarantine_row[0]),
            }
        except Exception:
            return {
                "raw_events": 0,
                "canonical_events": 0,
                "quarantine_events": 0,
            }
        finally:
            cursor.close()
            conn.close()

    def read_learning_metrics(self, horizon: str = "1M") -> dict[str, object]:
        """Return realized forecast quality metrics for learning-loop monitoring.

        Missing learning-loop tables must not crash operator dashboards.
        """
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        def to_int(value: object) -> int:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            if isinstance(value, str):
                return int(value)
            return 0

        def to_float_or_none(value: object) -> Optional[float]:
            if value is None:
                return None
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return float(value)
            return None

        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS forecast_count
                FROM forecast_records
                WHERE horizon = %s
                """,
                (horizon,),
            )
            forecast_row = cursor.fetchone() or (0,)

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS realized_count,
                    AVG(CASE WHEN rr.hit THEN 1.0 ELSE 0.0 END) AS hit_rate,
                    AVG(ABS(rr.forecast_error)) AS mean_abs_forecast_error,
                    AVG(rr.forecast_error) AS mean_signed_forecast_error
                FROM realization_records rr
                JOIN forecast_records fr ON fr.id = rr.forecast_id
                WHERE fr.horizon = %s
                """,
                (horizon,),
            )
            row = cursor.fetchone() or (0, None, None, None)

            forecast_count = to_int(forecast_row[0])
            realized_count = to_int(row[0])

            realization_coverage = None
            if forecast_count > 0:
                realization_coverage = realized_count / forecast_count

            return {
                "horizon": horizon,
                "forecast_count": forecast_count,
                "realized_count": realized_count,
                "realization_coverage": realization_coverage,
                "hit_rate": to_float_or_none(row[1]),
                "mean_abs_forecast_error": to_float_or_none(row[2]),
                "mean_signed_forecast_error": to_float_or_none(row[3]),
            }
        except Exception:
            return {
                "horizon": horizon,
                "forecast_count": 0,
                "realized_count": 0,
                "realization_coverage": None,
                "hit_rate": None,
                "mean_abs_forecast_error": None,
                "mean_signed_forecast_error": None,
            }
        finally:
            cursor.close()
            conn.close()

    def write_macro_series_points(self, points: list[NormalizedSeriesPoint]) -> int:
        if not points:
            return 0

        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()

        for point in points:
            cursor.execute(
                """
                INSERT INTO macro_series_points(
                    source,
                    entity_id,
                    metric_key,
                    as_of,
                    available_at,
                    value,
                    lineage_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    point.source,
                    point.entity_id,
                    point.metric_key,
                    point.as_of,
                    point.available_at,
                    point.value,
                    point.lineage_id,
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()
        return len(points)

    def read_macro_series_points(
        self,
        metric_key: str,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            SELECT source, entity_id, metric_key, as_of, available_at, value, lineage_id
            FROM macro_series_points
            WHERE metric_key = %s
            ORDER BY as_of DESC
            LIMIT %s
            """,
            (metric_key, limit),
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def read_latest_portfolio_exposure_snapshot(self) -> dict[str, object]:
        """Return latest paper-portfolio exposure metrics for policy checks.

        Expected metric_name keys in canonical_fact_store:
        - portfolio_exposure_leverage_weight
        - portfolio_exposure_crypto_total_weight
        - portfolio_exposure_crypto_btc_eth_weight
        - portfolio_exposure_crypto_alt_weight
        """
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        try:
            cursor.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (metric_name)
                        metric_name,
                        metric_value,
                        as_of,
                        source,
                        lineage_id
                    FROM canonical_fact_store
                    WHERE metric_name IN (
                        'portfolio_exposure_leverage_weight',
                        'portfolio_exposure_crypto_total_weight',
                        'portfolio_exposure_crypto_btc_eth_weight',
                        'portfolio_exposure_crypto_alt_weight'
                    )
                    ORDER BY metric_name, as_of DESC, ingested_at DESC
                )
                SELECT metric_name, metric_value, as_of, source, lineage_id
                FROM latest
                """,
                (),
            )
            rows = cursor.fetchall()
            if not rows:
                return {}

            payload: dict[str, object] = {}
            as_of_candidates: list[object] = []
            source_candidates: list[object] = []
            lineage_candidates: list[object] = []
            key_map = {
                "portfolio_exposure_leverage_weight": "leverage_weight",
                "portfolio_exposure_crypto_total_weight": "crypto_total_weight",
                "portfolio_exposure_crypto_btc_eth_weight": "crypto_btc_eth_weight",
                "portfolio_exposure_crypto_alt_weight": "crypto_alt_weight",
            }
            for metric_name, metric_value, as_of, source, lineage_id in rows:
                if metric_name in key_map:
                    payload[key_map[metric_name]] = metric_value
                if as_of is not None:
                    as_of_candidates.append(as_of)
                if source is not None:
                    source_candidates.append(source)
                if lineage_id is not None:
                    lineage_candidates.append(lineage_id)

            if as_of_candidates:
                payload["as_of"] = max(as_of_candidates)
            if source_candidates:
                payload["source"] = str(source_candidates[0])
            if lineage_candidates:
                payload["lineage_id"] = str(lineage_candidates[0])

            return payload
        except Exception:
            return {}
        finally:
            cursor.close()
            conn.close()

    def read_canonical_facts(
        self, source: str, metric_name: str, limit: int = 12
    ) -> list[dict[str, object]]:
        """Return up to *limit* rows from canonical_fact_store ordered by as_of asc.

        Used by CanonicalDataClient in the analysis layer.
        Returns [] when no connection is configured.
        """
        conn: ConnectionProtocol = self._connect()
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            SELECT
                source,
                entity_id,
                metric_name,
                metric_value,
                as_of,
                available_at,
                ingested_at,
                lineage_id
            FROM canonical_fact_store
            WHERE source = %s AND metric_name = %s
            ORDER BY as_of DESC
            LIMIT %s
            """,
            (source, metric_name, limit),
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        # Return chronological order (oldest first) for anomaly detection
        return list(reversed([dict(zip(columns, row)) for row in rows]))

    def snapshot_counts(self) -> dict[str, int]:
        return self.read_status_counters()
