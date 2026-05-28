"""
API Routes used for data retrieval
"""
from flask import Blueprint, jsonify
from flask import request
from db.db_util import get_db_connection, is_test_mode
from pipelines.gran_gov.ingestion_utils import normalize_grant_date

api_bp = Blueprint("api", __name__)


def _sort_rows_by_posted_date(rows: list[dict]) -> list[dict]:
    """Newest first; missing or unparsable posted_date last."""
    def key(r):
        # Empty string sorts below any YYYY-MM-DD; reverse=True puts dates first, nulls last.
        return normalize_grant_date(r.get("posted_date")) or ""
    return sorted(rows, key=key, reverse=True)

def _rows_to_dicts(cursor):
    """
    Convert DB rows into JSON-friendly dicts.
    psycopg dict_row already returns dict-like rows; do not zip(cols, row) on those
    (iterating a dict yields keys, which would map every column to its own name).
    """
    rows = cursor.fetchall()
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return [dict(r) for r in rows]
    cols = [desc[0] for desc in (cursor.description or [])]
    return [dict(zip(cols, row)) for row in rows]


def _row_to_dict(cursor):
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    cols = [desc[0] for desc in (cursor.description or [])]
    return dict(zip(cols, row))


def _as_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _tribal_rank_sql(alias: str = "tribal_eligibility") -> str:
    return (
        f"(COALESCE({alias}.relevancy_score, 0) + COALESCE({alias}.freshness_score, 0))"
    )


def _combined_rank_score(total_score, relevancy_score, freshness_score=None) -> float:
    return (
        (_as_float(total_score) or 0.0)
        + (_as_float(relevancy_score) or 0.0)
        + (_as_float(freshness_score) or 0.0)
    )


def _aggregate_tagged_opportunities(rows: list[dict]) -> list[dict]:
    """
    Collapse one SQL row per (opportunity, tag) into one dict per opportunity.

    Each item: opportunity_id, title, agency, status, estimated_funding,
    grant_gov_url, total_score, tag_scores, and optionally relevancy_score.
    Sorted by total_score + relevancy_score when relevancy_score is present.
    """
    by_oid: dict[str, dict] = {}
    for r in rows:
        oid = r.get("opportunity_id")
        oid_s = str(oid) if oid is not None else ""
        if not oid_s:
            continue
        if oid_s not in by_oid:
            by_oid[oid_s] = {
                "opportunity_id": oid,
                "title": r.get("title"),
                "agency": r.get("agency"),
                "status": r.get("status"),
                "estimated_funding": r.get("estimated_funding"),
                "grant_gov_url": r.get("grant_gov_url"),
                "total_score": _as_float(r.get("total_score")),
                "relevancy_score": _as_float(r.get("relevancy_score")),
                "freshness_score": _as_float(r.get("freshness_score")),
                "_tag_best": {},
            }
        g = by_oid[oid_s]
        if r.get("total_score") is not None:
            g["total_score"] = _as_float(r.get("total_score"))
        if r.get("relevancy_score") is not None:
            g["relevancy_score"] = _as_float(r.get("relevancy_score"))
        if r.get("freshness_score") is not None:
            g["freshness_score"] = _as_float(r.get("freshness_score"))
        tag = r.get("tag")
        if tag is None:
            continue
        tag_s = str(tag)
        ts = _as_float(r.get("tag_score"))
        prev = g["_tag_best"].get(tag_s)
        if prev is None or (ts is not None and (prev is None or ts > prev)):
            g["_tag_best"][tag_s] = ts
    out: list[dict] = []
    for g in by_oid.values():
        tag_best = g.pop("_tag_best")
        tag_scores = [{"tag": t, "tag_score": s} for t, s in tag_best.items()]
        tag_scores.sort(
            key=lambda x: (
                x["tag_score"] if x["tag_score"] is not None else 0.0,
                x["tag"],
            ),
            reverse=True,
        )
        g["tag_scores"] = tag_scores
        out.append(g)
    out.sort(
        key=lambda x: _combined_rank_score(
            x.get("total_score"), x.get("relevancy_score"), x.get("freshness_score")
        ),
        reverse=True,
    )
    return out


@api_bp.route("/api/opportunities")
def get_opportunities():
    """
    List tribal-eligible opportunities with status posted or forecasted (open only).

    Without ``tags`` or ``q``: up to 50 rows (opportunity_id, title, description, agency,
    status, estimated_funding, grant_gov_url).

    With ``q``: title/agency ILIKE search; newest ``posted_date`` first (parsed, not string sort).

    With ``tags`` (comma-separated): aggregated per opportunity, ordered by total_score
    descending, with tag_scores for matching tags. Closed/archived grants are excluded.
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        tags_raw = request.args.get("tags", "")
        tag_list = [t.strip() for t in tags_raw.split(",") if t.strip()]
        q_raw = (request.args.get("q") or "").strip()
        if tag_list:
            tag_list_lower = [t.lower() for t in tag_list]
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    grants.opportunity_id,
                    grants.title,
                    grants.description,
                    grants.agency,
                    grants.status,
                    grants.estimated_funding,
                    grant_tags.tag,
                    grant_tags.tag_score,
                    grant_tags.total_score,
                    grants.grant_gov_url
                FROM grants
                INNER JOIN (
                    SELECT
                        opportunity_id,
                        tag,
                        tag_score,
                        sum(tag_score) OVER (PARTITION BY opportunity_id) AS total_score
                    FROM grant_tags
                    WHERE LOWER(tag) = ANY(%s)
                ) AS grant_tags
                    ON grants.opportunity_id = grant_tags.opportunity_id
                left join tribal_eligibility on grants.opportunity_id = tribal_eligibility.opportunity_id
                WHERE grant_tags.total_score > 0 and grants.status in ('posted', 'forecasted') and tribal_eligibility.is_tribal_eligible = true
                ORDER BY grant_tags.total_score DESC
                """,
                (tag_list_lower,),
            )
            raw = _rows_to_dicts(cursor)
            opportunities = _aggregate_tagged_opportunities(raw)
        elif q_raw:
            cursor = conn.cursor()
            pattern = f"%{q_raw}%"
            cursor.execute(
                """
                SELECT
                    grants.opportunity_id, 
                    grants.title, 
                    grants.description,
                    grants.agency, 
                    grants.status,
                    grants.posted_date,
                    grants.estimated_funding, 
                    grants.grant_gov_url
                FROM grants
                LEFT JOIN tribal_eligibility
                    ON grants.opportunity_id = tribal_eligibility.opportunity_id
                WHERE (grants.title ILIKE %s OR grants.agency ILIKE %s)
                    AND tribal_eligibility.is_tribal_eligible = true
                    AND grants.status IN ('posted', 'forecasted')
                ORDER BY (COALESCE(tribal_eligibility.relevancy_score, 0) + COALESCE(tribal_eligibility.freshness_score, 0)) DESC NULLS LAST
                """,
                (pattern, pattern),
            )
            opportunities = _sort_rows_by_posted_date(_rows_to_dicts(cursor))[:50]
        else:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    grants.opportunity_id, 
                    grants.title, 
                    grants.description, 
                    grants.agency, 
                    grants.status,
                    grants.posted_date,
                    grants.estimated_funding, 
                    grants.grant_gov_url 
                FROM grants 
                LEFT JOIN tribal_eligibility
                    ON grants.opportunity_id = tribal_eligibility.opportunity_id
                WHERE tribal_eligibility.is_tribal_eligible = true
                    AND grants.status IN ('posted', 'forecasted')
                ORDER BY (COALESCE(tribal_eligibility.relevancy_score, 0) + COALESCE(tribal_eligibility.freshness_score, 0)) DESC NULLS LAST
                LIMIT 100
                """
            )
            opportunities = _sort_rows_by_posted_date(_rows_to_dicts(cursor))[:50]
        return jsonify(opportunities)
    except Exception as e:
        return jsonify({"message": "Error getting opportunities: " + str(e)}), 500
    finally:
        conn.close()

@api_bp.route("/api/get_opportunities_v2")
def get_opportunities_v2():
    """
    Tribal-eligible open grants.

    With ``tags``: sorted by tag scores + tribal relevancy + freshness.
    Without ``tags`` (default or ``q``): sorted by tribal relevancy + freshness.
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cursor = conn.cursor()
        tags_raw = request.args.get("tags", "")
        tag_list = [t.strip() for t in tags_raw.split(",") if t.strip()]
        q_raw = (request.args.get("q") or "").strip()
        if tag_list:
            tag_list_lower = [t.lower() for t in tag_list]
            tribal_rank = _tribal_rank_sql()
            cursor.execute(
                f"""
                SELECT
                    grants.opportunity_id,
                    grants.title,
                    grants.description,
                    grants.agency,
                    grants.status,
                    grants.estimated_funding,
                    grant_tags.tag,
                    grant_tags.tag_score,
                    grant_tags.total_score,
                    grants.grant_gov_url,
                    tribal_eligibility.relevancy_score,
                    tribal_eligibility.freshness_score
                FROM grants
                INNER JOIN (
                    SELECT
                        opportunity_id,
                        tag,
                        tag_score,
                        SUM(tag_score) OVER (PARTITION BY opportunity_id) AS total_score
                    FROM grant_tags
                    WHERE LOWER(tag) = ANY(%s)
                ) AS grant_tags
                    ON grants.opportunity_id = grant_tags.opportunity_id
                LEFT JOIN tribal_eligibility
                    ON grants.opportunity_id = tribal_eligibility.opportunity_id
                WHERE grant_tags.total_score > 0
                    AND grants.status IN ('posted', 'forecasted')
                    AND tribal_eligibility.is_tribal_eligible = true
                ORDER BY (
                    grant_tags.total_score + {tribal_rank}
                ) DESC
                """,
                (tag_list_lower,),
            )
            raw = _rows_to_dicts(cursor)
            opportunities = _aggregate_tagged_opportunities(raw)
        elif q_raw:
            pattern = f"%{q_raw}%"
            cursor.execute(
                f"""
                SELECT
                    grants.opportunity_id,
                    grants.title,
                    grants.description,
                    grants.agency,
                    grants.status,
                    grants.posted_date,
                    grants.estimated_funding,
                    grants.grant_gov_url,
                    tribal_eligibility.relevancy_score,
                    tribal_eligibility.freshness_score
                FROM grants
                LEFT JOIN tribal_eligibility
                    ON grants.opportunity_id = tribal_eligibility.opportunity_id
                WHERE (grants.title ILIKE %s OR grants.agency ILIKE %s)
                    AND tribal_eligibility.is_tribal_eligible = true
                    AND grants.status IN ('posted', 'forecasted')
                ORDER BY {_tribal_rank_sql()} DESC NULLS LAST
                LIMIT 50
                """,
                (pattern, pattern),
            )
            opportunities = _rows_to_dicts(cursor)
        else:
            cursor.execute(
                f"""
                SELECT
                    grants.opportunity_id,
                    grants.title,
                    grants.description,
                    grants.agency,
                    grants.status,
                    grants.posted_date,
                    grants.estimated_funding,
                    grants.grant_gov_url,
                    tribal_eligibility.relevancy_score,
                    tribal_eligibility.freshness_score
                FROM grants
                LEFT JOIN tribal_eligibility
                    ON grants.opportunity_id = tribal_eligibility.opportunity_id
                WHERE tribal_eligibility.is_tribal_eligible = true
                    AND grants.status IN ('posted', 'forecasted')
                ORDER BY {_tribal_rank_sql()} DESC NULLS LAST
                LIMIT 100
                """
            )
            opportunities = _rows_to_dicts(cursor)
        return jsonify(opportunities)
    except Exception as e:
        return jsonify({"message": "Error getting opportunities: " + str(e)}), 500
    finally:
        conn.close()


@api_bp.route("/api/opportunities/total_funding")
def get_total_funding():
    """
    Get the total funding amount for an opportunity from the database (grants table)
    Returns:
        total funding amount by tag or agency
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        tag_raw = request.args.get("tag", "")
        tag_list = [t.strip() for t in tag_raw.split(",") if t.strip()]
        cursor = conn.cursor()
        if tag_list:
            tag_list_lower = [t.lower() for t in tag_list]
            cursor.execute(
                """
                SELECT
                    grant_tags.tag,
                    SUM(COALESCE(grants.estimated_funding, 0)) AS total_funding,
                    COUNT(grants.opportunity_id) AS total_grants
                FROM grants
                INNER JOIN (
                    SELECT
                        opportunity_id,
                        tag,
                        tag_score,
                        SUM(tag_score) OVER (PARTITION BY opportunity_id) AS total_score
                    FROM grant_tags
                    WHERE LOWER(tag) = ANY(%s)
                ) AS grant_tags
                    ON grants.opportunity_id = grant_tags.opportunity_id
                WHERE grant_tags.total_score > 0
                GROUP BY grant_tags.tag
                ORDER BY total_funding DESC NULLS LAST
                """,
                (tag_list_lower,),
            )
            total_funding = _rows_to_dicts(cursor)
            return jsonify(total_funding)
        else:
            cursor.execute(
                """
                SELECT agency,
                       SUM(COALESCE(estimated_funding, 0)) AS total_funding,
                       COUNT(opportunity_id) AS total_grants
                FROM grants
                GROUP BY agency
                ORDER BY total_funding DESC NULLS LAST
                """
            )
            total_funding = _rows_to_dicts(cursor)
            return jsonify(total_funding)
    except Exception as e:
        return jsonify({"message": "Error getting total funding: " + str(e)}), 500
    finally:
        conn.close()

@api_bp.route("/api/opportunities/<opportunity_id>")
def get_opportunity_by_id(opportunity_id):
    """
    Get an opportunity by id from the database (grants table)
    Returns:
        opportunity
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM grants WHERE opportunity_id = %s", (opportunity_id,))
        opportunity = _row_to_dict(cursor)
        if opportunity is None:
            return jsonify({}), 404
        return jsonify(opportunity)
    except Exception as e:
        return jsonify({"message": "Error getting opportunity by id: " + str(e)}), 500
    finally:
        conn.close()

@api_bp.route("/api/alerts")
def get_alerts():
    """
    Get 50 most recent alerts from the database (grant_alerts table)
    Returns:
        list of alerts
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM grant_alerts order by detected_at desc limit 50")
        alerts = _rows_to_dicts(cursor)
        return jsonify(alerts)
    except Exception as e:
        return jsonify({"message": "Error getting alerts: " + str(e)}), 500
    finally:
        conn.close()
