from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from difflib import SequenceMatcher
from typing import Any, Optional


MONEY_EPSILON = 0.01  # ignore tiny float noise


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_iso_date(x: Any) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    s = str(x).strip()
    if not s:
        return None
    # Accept ISO with optional time
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        # Best-effort cleanup (e.g., "Mar 16, 2026")
        s2 = re.sub(r"\s+", " ", s)
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s2, fmt).date()
            except ValueError:
                pass
    return None


def _as_set_list(x: Any) -> list[str]:
    if not x:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]

    # If normalize stored JSON text for list fields (e.g. `json.dumps([...])`),
    # parse it so comparisons are semantic instead of comma-splitting the raw JSON.
    if isinstance(x, str):
        s = x.strip()
        if s:
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    out: list[str] = []
                    for i in parsed:
                        if i is None:
                            continue
                        if isinstance(i, (dict, list)):
                            out.append(json.dumps(i, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                        else:
                            si = str(i).strip()
                            if si:
                                out.append(si)
                    return out
            except json.JSONDecodeError:
                # fall back to comma-separated string behavior below
                pass

    # fallback if someone stored comma-separated
    s = str(x).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _meaningful_description_change(
    old_desc: str,
    new_desc: str,
    *,
    min_length_change_ratio: float = 0.08,
    similarity_threshold: float = 0.90,
) -> bool:
    old_desc = (old_desc or "").strip()
    new_desc = (new_desc or "").strip()
    if not old_desc and not new_desc:
        return False
    if old_desc == new_desc:
        return False

    # Normalize whitespace only (not semantics)
    old_n = re.sub(r"\s+", " ", old_desc)
    new_n = re.sub(r"\s+", " ", new_desc)

    if old_n == new_n:
        return False

    len_old = len(old_n)
    len_new = len(new_n)
    if len_old == 0 or len_new == 0:
        return True  # one side empty => meaningful

    length_ratio = abs(len_new - len_old) / max(len_old, len_new)
    cap = 8000  # performance guard
    similarity = SequenceMatcher(None, old_n[:cap], new_n[:cap]).ratio()

    # Trigger if length changed “enough” OR similarity dropped “enough”
    return (length_ratio >= min_length_change_ratio) or (similarity < similarity_threshold)


@dataclass(frozen=True)
class Alert:
    type: str
    field: str
    old_value: Any
    new_value: Any


def detect_changes(old_data: dict[str, Any], new_data: dict[str, Any]) -> list[dict[str, Any]]:
    alerts: list[Alert] = []

    # 1) deadline extended/shortened
    old_close = _parse_iso_date(old_data.get("close_date"))
    new_close = _parse_iso_date(new_data.get("close_date"))
    if old_close and new_close:
        if new_close > old_close:
            alerts.append(Alert(
                type="deadline_extended",
                field="close_date",
                old_value=old_close.isoformat(),
                new_value=new_close.isoformat(),
            ))
        elif new_close < old_close:
            alerts.append(Alert(
                type="deadline_shortened",
                field="close_date",
                old_value=old_close.isoformat(),
                new_value=new_close.isoformat(),
            ))

    # 2) status changed
    old_status = str(old_data.get("status", "")).strip().lower()
    new_status = str(new_data.get("status", "")).strip().lower()
    if old_status and new_status and old_status != new_status:
        alerts.append(Alert(
            type="status_changed",
            field="status",
            old_value=old_status,
            new_value=new_status,
        ))

    # 3) funding increased/decreased (award_ceiling, award_floor)
    for field in ("award_ceiling", "estimated_funding", "award_floor", "cost_sharing"):
        old_val = _safe_float(old_data.get(field))
        new_val = _safe_float(new_data.get(field))
        if old_val is None or new_val is None:
            continue

        diff = new_val - old_val
        if diff >= MONEY_EPSILON:
            alerts.append(Alert(
                type="funding_increased",
                field=field,
                old_value=round(old_val, 2),
                new_value=round(new_val, 2),
            ))
        elif diff <= -MONEY_EPSILON:
            alerts.append(Alert(
                type="funding_decreased",
                field=field,
                old_value=round(old_val, 2),
                new_value=round(new_val, 2),
            ))

    # 4) eligibility changed
    old_elig = set(_as_set_list(old_data.get("eligibilities")))
    new_elig = set(_as_set_list(new_data.get("eligibilities")))
    if old_elig != new_elig:
        alerts.append(Alert(
            type="eligibility_changed",
            field="eligibilities",
            old_value=json.dumps(sorted(old_elig)),
            new_value=json.dumps(sorted(new_elig)),
        ))

    # 5) new attachments added
    old_att = set(_as_set_list(old_data.get("attachments")))
    new_att = set(_as_set_list(new_data.get("attachments")))
    added = sorted(new_att - old_att)
    if added:
        alerts.append(Alert(
            type="new_attachments_added",
            field="attachments_added",
            old_value=json.dumps([]),
            new_value=json.dumps(added),
        ))

    # 6) description updated (meaningful only)
    old_desc = str(old_data.get("description") or "")
    new_desc = str(new_data.get("description") or "")
    if _meaningful_description_change(old_desc, new_desc):
        alerts.append(Alert(
            type="description_updated",
            field="description",
            old_value=old_desc,
            new_value=new_desc,
        ))

    return [
        {"type": a.type, "field": a.field, "old_value": a.old_value, "new_value": a.new_value}
        for a in alerts
    ]