import hashlib
import json
import logging
import os
import time
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from services.shared.database import db_session


pytestmark = pytest.mark.e2e

_DEFAULT_BASE_URL = "http://localhost:8000"
_DEFAULT_POLL_SECONDS = 3
_DEFAULT_TIMEOUT_SECONDS = 60 * 30

logger = logging.getLogger("e2e_real_sync")


def _e2e_log_enabled():
    return os.getenv("WAVE_METRICS_E2E_LOG", "").strip() == "1"


def _e2e_log(message, **fields):
    if not _e2e_log_enabled():
        return
    safe_fields = {k: v for k, v in fields.items() if v is not None}
    logger.info(message, extra=safe_fields)


def _token_fingerprint(token):
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()[:8]


def _load_valid_outputs_by_token():
    """
    Load expected outputs keyed by GitHub OAuth App token or PAT

    This file is intentionally local-only and must not be committed.

    Returns:
        Dict keyed by token
    """
    override_path = os.getenv("WAVE_METRICS_VALID_OUTPUTS_PATH", "").strip()
    if not override_path:
        pytest.skip(
            "Real E2E tests require WAVE_METRICS_VALID_OUTPUTS_PATH to be set to a local-only expected outputs JSON"
        )

    p = Path(override_path)
    if not p.is_file():
        pytest.skip(
            f"Real E2E tests require WAVE_METRICS_VALID_OUTPUTS_PATH to point to a readable file; got {override_path!r}"
        )

    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _require_e2e_enabled():
    if os.getenv("WAVE_METRICS_E2E", "").strip() != "1":
        pytest.skip("Set WAVE_METRICS_E2E=1 to enable real end-to-end tests")


def _http_client():
    base_url = os.getenv("WAVE_METRICS_BASE_URL", _DEFAULT_BASE_URL).strip()

    api_auth_token = os.getenv("WAVE_METRICS_API_AUTH_TOKEN", "").strip()
    if not api_auth_token:
        api_auth_token = os.getenv("API_AUTH_TOKEN", "").strip()

    headers = {}
    if api_auth_token:
        headers["Authorization"] = f"Bearer {api_auth_token}"

    return httpx.Client(base_url=base_url, headers=headers, timeout=120.0)


def _wait_for_job(client, job_id, token_fingerprint, context):
    timeout_seconds = int(os.getenv("WAVE_METRICS_E2E_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT_SECONDS)))
    poll_seconds = float(os.getenv("WAVE_METRICS_E2E_POLL_SECONDS", str(_DEFAULT_POLL_SECONDS)))

    start = time.time()
    last_payload = None
    last_status = None

    _e2e_log(
        "e2e: polling job",
        token_fingerprint=token_fingerprint,
        job_id=job_id,
        ctx=context,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )

    while True:
        resp = client.get(f"/api/v1/jobs/{job_id}")
        assert resp.status_code == 200, f"token={token_fingerprint} ctx={context} {resp.text}"

        payload = resp.json()
        last_payload = payload

        status = payload.get("status")
        if status != last_status:
            last_status = status
            _e2e_log(
                "e2e: job status",
                token_fingerprint=token_fingerprint,
                job_id=job_id,
                ctx=context,
                status=status,
            )

        if status in {"COMPLETED", "FAILED", "SKIPPED"}:
            duration_seconds = time.time() - start
            _e2e_log(
                "e2e: job terminal",
                token_fingerprint=token_fingerprint,
                job_id=job_id,
                ctx=context,
                status=status,
                duration_seconds=round(duration_seconds, 3),
            )

            if status == "FAILED":
                raise AssertionError(
                    f"token={token_fingerprint} ctx={context} job={job_id} failed "
                    f"error={payload.get('error_message')} last={payload}"
                )

            if status == "SKIPPED":
                raise AssertionError(
                    f"token={token_fingerprint} ctx={context} job={job_id} skipped "
                    f"error={payload.get('error_message')} last={payload}"
                )

            return {"job": payload, "duration_seconds": round(duration_seconds, 3)}

        if time.time() - start > timeout_seconds:
            raise AssertionError(f"token={token_fingerprint} ctx={context} timed out job={job_id} last={last_payload}")

        time.sleep(poll_seconds)


def _find_expected_metrics_response(obj):
    if isinstance(obj, dict) and "metrics" in obj and "github_login" in obj:
        return obj

    required = {
        "user_id",
        "github_login",
        "metrics_window_start",
        "metrics_window_end",
        "metrics_baseline_dates",
        "metrics",
        "lifetime_language_profile",
        "computed_at",
    }

    candidates = []

    def _walk(node):
        if isinstance(node, dict):
            if required.issubset(set(node.keys())):
                candidates.append(node)
            for v in node.values():
                _walk(v)
            return

        if isinstance(node, list):
            for v in node:
                _walk(v)

    _walk(obj)

    if not candidates:
        raise AssertionError("expected at least one MetricsResponse in expected outputs")

    return candidates[0]


def _normalize_metrics_response(payload):
    """
    Remove fields that are expected to vary between runs

    Returns:
        Dict safe to compare across runs
    """
    scrubbed = json.loads(json.dumps(payload))

    for key in {"user_id", "computed_at", "metrics_window_start", "metrics_window_end"}:
        scrubbed.pop(key, None)

    metrics = scrubbed.get("metrics") or {}
    if isinstance(metrics, dict):
        for _metric_name, entry in metrics.items():
            if not isinstance(entry, dict):
                continue
            entry.pop("description", None)

    return scrubbed


def _float_tolerance_for_metric(metric_name, expected_value):
    expected_value_f = float(expected_value)

    float_rel_tol_default = float(os.getenv("WAVE_METRICS_E2E_FLOAT_REL_TOL", "0.05"))
    float_abs_tol_small = float(os.getenv("WAVE_METRICS_E2E_FLOAT_ABS_TOL_SMALL", "0.05"))
    float_abs_switch = float(os.getenv("WAVE_METRICS_E2E_FLOAT_ABS_SWITCH", "0.2"))

    rate_abs_tol = float(os.getenv("WAVE_METRICS_E2E_RATE_ABS_TOL", "0.05"))
    latency_rel_tol = float(os.getenv("WAVE_METRICS_E2E_LATENCY_REL_TOL", "0.15"))

    if metric_name in {"pr_merge_rate", "pr_drop_rate"}:
        return "abs", rate_abs_tol

    if metric_name == "avg_merge_latency_hours":
        return "rel", latency_rel_tol

    if abs(expected_value_f) <= float_abs_switch:
        return "abs", float_abs_tol_small

    return "rel", float_rel_tol_default


def _assert_metrics_close(actual, expected, token_fingerprint, context):
    actual_metrics = (actual.get("metrics") or {}) if isinstance(actual, dict) else {}
    expected_metrics = (expected.get("metrics") or {}) if isinstance(expected, dict) else {}

    if actual.get("metrics_baseline_dates") != expected.get("metrics_baseline_dates"):
        raise AssertionError(
            f"baseline dates mismatch token={token_fingerprint} ctx={context} "
            f"actual={actual.get('metrics_baseline_dates')} expected={expected.get('metrics_baseline_dates')}"
        )

    int_abs_tol = int(os.getenv("WAVE_METRICS_E2E_INT_ABS_TOL", "10"))
    pct_abs_tol = float(os.getenv("WAVE_METRICS_E2E_PERCENTILE_ABS_TOL", "1.0"))

    mismatches = []

    for metric_name, expected_entry in expected_metrics.items():
        actual_entry = actual_metrics.get(metric_name)
        if not isinstance(expected_entry, dict) or not isinstance(actual_entry, dict):
            mismatches.append(
                f"metric={metric_name} missing/invalid expected_type={type(expected_entry).__name__} "
                f"actual_type={type(actual_entry).__name__}"
            )
            continue

        expected_value = expected_entry.get("value")
        actual_value = actual_entry.get("value")

        if expected_value is None:
            if actual_value is not None:
                mismatches.append(f"metric={metric_name} value expected=None actual={actual_value}")
        elif isinstance(expected_value, int):
            if not isinstance(actual_value, int):
                mismatches.append(
                    f"metric={metric_name} value expected_int={expected_value} "
                    f"actual_type={type(actual_value).__name__}"
                )
            else:
                delta = int(actual_value) - int(expected_value)
                if abs(delta) > int_abs_tol:
                    mismatches.append(
                        f"metric={metric_name} value expected={expected_value} actual={actual_value} "
                        f"delta={delta} tol=±{int_abs_tol}"
                    )
        elif isinstance(expected_value, float):
            if not isinstance(actual_value, (int, float)):
                mismatches.append(
                    f"metric={metric_name} value expected_float={expected_value} "
                    f"actual_type={type(actual_value).__name__}"
                )
            else:
                expected_value_f = float(expected_value)
                actual_value_f = float(actual_value)

                if expected_value_f == 0.0:
                    if abs(actual_value_f - expected_value_f) > 1e-9:
                        mismatches.append(
                            f"metric={metric_name} value expected={expected_value_f} actual={actual_value_f} "
                            f"abs_diff={abs(actual_value_f - expected_value_f):.6g} tol=1e-9"
                        )
                else:
                    mode, tol = _float_tolerance_for_metric(metric_name, expected_value_f)
                    abs_diff = abs(actual_value_f - expected_value_f)

                    if mode == "abs":
                        if abs_diff > tol:
                            mismatches.append(
                                f"metric={metric_name} value expected={expected_value_f} actual={actual_value_f} "
                                f"abs_diff={abs_diff:.6g} tol=±{tol}"
                            )
                    else:
                        rel = abs_diff / abs(expected_value_f)
                        if rel > tol:
                            mismatches.append(
                                f"metric={metric_name} value expected={expected_value_f} actual={actual_value_f} "
                                f"rel_diff={rel:.6g} tol≤{tol}"
                            )
        else:
            if actual_value != expected_value:
                mismatches.append(f"metric={metric_name} value expected={expected_value!r} actual={actual_value!r}")

        expected_pct = expected_entry.get("percentile")
        actual_pct = actual_entry.get("percentile")

        if expected_pct is None:
            if actual_pct is not None:
                mismatches.append(f"metric={metric_name} percentile expected=None actual={actual_pct}")
        else:
            if actual_pct is None:
                mismatches.append(f"metric={metric_name} percentile expected={expected_pct} actual=None")
            else:
                pct_diff = abs(float(actual_pct) - float(expected_pct))
                if pct_diff > pct_abs_tol:
                    mismatches.append(
                        f"metric={metric_name} percentile expected={expected_pct} actual={actual_pct} "
                        f"abs_diff={pct_diff:.6g} tol=±{pct_abs_tol}"
                    )

    if mismatches:
        preview = "\n".join(mismatches[:12])
        raise AssertionError(
            f"metrics mismatch token={token_fingerprint} ctx={context} mismatches={len(mismatches)}\n{preview}"
        )


def _get_sync_job_backfill_days(job_id):
    with db_session() as session:
        row = session.execute(
            text("SELECT backfill_days FROM sync_jobs WHERE job_id=:job_id"),
            {"job_id": job_id},
        ).fetchone()

    assert row is not None
    return row[0]


def _fetch_github_viewer_identity(token):
    """
    Fetch the GitHub viewer identity for a given OAuth App token or PAT

    Args:
        token (str): GitHub OAuth App token or PAT

    Returns:
        Tuple of (login_lower, github_user_id)
    """
    token_fp = _token_fingerprint(token)
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"query": "query{ viewer { login databaseId } }", "variables": {}}

    resp = httpx.post("https://api.github.com/graphql", headers=headers, json=payload, timeout=60.0)
    if resp.status_code != 200:
        raise AssertionError(f"token={token_fp} GitHub graphql failed status={resp.status_code} body={resp.text[:300]}")

    body = resp.json() if resp.content else {}
    errors = body.get("errors") or []
    if errors:
        raise AssertionError(f"token={token_fp} GitHub graphql errors={errors[:1]}")

    viewer = (body.get("data") or {}).get("viewer") or {}
    login = str(viewer.get("login") or "").strip().lower()
    github_user_id = int(viewer.get("databaseId") or 0)

    if not login or github_user_id <= 0:
        raise AssertionError(f"token={token_fp} invalid viewer response login={login!r} databaseId={github_user_id}")

    return login, github_user_id


def _clear_github_sync_state_for_github_user_id(github_user_id):
    with db_session() as session:
        result = session.execute(
            text(
                "DELETE FROM github_sync_state gss "
                "USING users u "
                "WHERE gss.user_id = u.id "
                "  AND u.github_user_id = :gh_uid"
            ),
            {"gh_uid": int(github_user_id)},
        )

    return int(result.rowcount or 0)


def test_real_e2e_sync_variants_then_metrics_expected():
    _require_e2e_enabled()
    valid = _load_valid_outputs_by_token()

    summaries = []

    with _http_client() as client:
        health = client.get("/health")
        assert health.status_code == 200, health.text

        try:
            for token, expected_blob in valid.items():
                token_fp = _token_fingerprint(token)
                token_summary = {
                    "token_fingerprint": token_fp,
                    "jobs": [],
                    "metrics_fetches": [],
                }

                expected_metrics_full = _find_expected_metrics_response(expected_blob)
                expected_norm = _normalize_metrics_response(expected_metrics_full)

                _e2e_log("e2e: start token", token_fingerprint=token_fp)

                try:
                    start_t = time.time()
                    resp = client.post("/api/v1/sync", json={"github_token": token})
                    elapsed_ms = int((time.time() - start_t) * 1000)
                    assert resp.status_code == 200, f"token={token_fp} {resp.text}"

                    p0 = resp.json()
                    job_id_0 = p0["job_id"]
                    user_id = p0["user_id"]
                    github_login = str(p0.get("github_login") or "").strip().lower()
                    assert github_login

                    token_summary["initial"] = {
                        "user_id": user_id,
                        "github_login": github_login,
                        "sync_response_ms": elapsed_ms,
                    }

                    _e2e_log(
                        "e2e: sync enqueued",
                        token_fingerprint=token_fp,
                        ctx="variant=token_only",
                        job_id=job_id_0,
                        user_id=user_id,
                        github_login=github_login,
                        sync_response_ms=elapsed_ms,
                    )

                    w0 = _wait_for_job(client, job_id_0, token_fingerprint=token_fp, context="variant=token_only")
                    token_summary["jobs"].append({"ctx": "variant=token_only", **w0, "sync_response_ms": elapsed_ms})

                    start_t = time.time()
                    resp = client.post("/api/v1/sync", json={"user_id": user_id, "github_token": token})
                    elapsed_ms = int((time.time() - start_t) * 1000)
                    assert resp.status_code == 200, f"token={token_fp} {resp.text}"
                    p1 = resp.json()

                    _e2e_log(
                        "e2e: sync enqueued",
                        token_fingerprint=token_fp,
                        ctx="variant=user_id_only",
                        job_id=p1.get("job_id"),
                        user_id=p1.get("user_id"),
                        github_login=str(p1.get("github_login") or "").strip().lower(),
                        sync_response_ms=elapsed_ms,
                    )

                    w1 = _wait_for_job(
                        client,
                        p1["job_id"],
                        token_fingerprint=token_fp,
                        context="variant=user_id_only",
                    )
                    token_summary["jobs"].append(
                        {
                            "ctx": "variant=user_id_only",
                            **w1,
                            "sync_response_ms": elapsed_ms,
                        }
                    )

                    start_t = time.time()
                    resp = client.post(
                        "/api/v1/sync",
                        json={"github_login": github_login, "github_token": token},
                    )
                    elapsed_ms = int((time.time() - start_t) * 1000)
                    assert resp.status_code == 200, f"token={token_fp} {resp.text}"
                    p2 = resp.json()

                    _e2e_log(
                        "e2e: sync enqueued",
                        token_fingerprint=token_fp,
                        ctx="variant=github_login_only",
                        job_id=p2.get("job_id"),
                        user_id=p2.get("user_id"),
                        github_login=str(p2.get("github_login") or "").strip().lower(),
                        sync_response_ms=elapsed_ms,
                    )

                    w2 = _wait_for_job(
                        client,
                        p2["job_id"],
                        token_fingerprint=token_fp,
                        context="variant=github_login_only",
                    )
                    token_summary["jobs"].append(
                        {
                            "ctx": "variant=github_login_only",
                            **w2,
                            "sync_response_ms": elapsed_ms,
                        }
                    )

                    for resolved_user_id, resolved_login in [
                        (user_id, github_login),
                        (p1["user_id"], str(p1.get("github_login") or "").strip().lower()),
                        (p2["user_id"], str(p2.get("github_login") or "").strip().lower()),
                    ]:
                        assert resolved_login

                        start_t = time.time()
                        metrics_by_id = client.get("/api/v1/metrics", params={"user_id": resolved_user_id})
                        by_id_ms = int((time.time() - start_t) * 1000)
                        assert metrics_by_id.status_code == 200, f"token={token_fp} {metrics_by_id.text}"

                        start_t = time.time()
                        metrics_by_login = client.get(
                            "/api/v1/metrics/by-login",
                            params={"github_login": resolved_login},
                        )
                        by_login_ms = int((time.time() - start_t) * 1000)
                        assert metrics_by_login.status_code == 200, f"token={token_fp} {metrics_by_login.text}"

                        actual_a_raw = metrics_by_id.json()
                        actual_b_raw = metrics_by_login.json()

                        metrics_count = len((actual_a_raw.get("metrics") or {}))
                        baseline_dates = actual_a_raw.get("metrics_baseline_dates")

                        _e2e_log(
                            "e2e: fetch metrics",
                            token_fingerprint=token_fp,
                            user_id=resolved_user_id,
                            github_login=resolved_login,
                            metrics_by_id_ms=by_id_ms,
                            metrics_by_login_ms=by_login_ms,
                            metrics_count=metrics_count,
                            baseline_dates=baseline_dates,
                        )

                        token_summary["metrics_fetches"].append(
                            {
                                "user_id": resolved_user_id,
                                "github_login": resolved_login,
                                "metrics_by_id_ms": by_id_ms,
                                "metrics_by_login_ms": by_login_ms,
                                "metrics_count": metrics_count,
                                "baseline_dates": baseline_dates,
                            }
                        )

                        actual_a = _normalize_metrics_response(actual_a_raw)
                        actual_b = _normalize_metrics_response(actual_b_raw)
                        assert actual_a == actual_b

                        _assert_metrics_close(
                            actual_a,
                            expected_norm,
                            token_fp,
                            context=f"resolved_user_id={resolved_user_id} login={resolved_login}",
                        )

                    token_summary["status"] = "passed"
                except Exception as exc:
                    token_summary["status"] = "failed"
                    token_summary["error"] = str(exc)
                    raise
                finally:
                    summaries.append(token_summary)
                    _e2e_log("e2e: done token", token_fingerprint=token_fp)

                    if _e2e_log_enabled():
                        logger.info("e2e: token summary\n%s", json.dumps(token_summary, indent=2, sort_keys=True))
        finally:
            if _e2e_log_enabled():
                logger.info("e2e: summary sync variants\n%s", json.dumps(summaries, indent=2, sort_keys=True))


def test_real_e2e_incremental_sync_after_first_backfill_expected():
    _require_e2e_enabled()
    valid = _load_valid_outputs_by_token()

    summaries = []

    with _http_client() as client:
        try:
            for token, _expected_blob in valid.items():
                token_fp = _token_fingerprint(token)
                token_summary = {"token_fingerprint": token_fp}

                _e2e_log("e2e: start token", token_fingerprint=token_fp)

                try:
                    viewer_login, github_user_id = _fetch_github_viewer_identity(token)
                    deleted = _clear_github_sync_state_for_github_user_id(github_user_id)

                    token_summary["viewer"] = {
                        "github_login": viewer_login,
                        "github_user_id": github_user_id,
                        "deleted_github_sync_state_rows": deleted,
                    }

                    _e2e_log(
                        "e2e: cleared github_sync_state",
                        token_fingerprint=token_fp,
                        github_login=viewer_login,
                        github_user_id=github_user_id,
                        deleted_rows=deleted,
                    )

                    start_t = time.time()
                    resp1 = client.post("/api/v1/sync", json={"github_token": token})
                    elapsed_ms = int((time.time() - start_t) * 1000)
                    assert resp1.status_code == 200, f"token={token_fp} {resp1.text}"
                    p1 = resp1.json()

                    token_summary["first_sync"] = {
                        "job_id": p1.get("job_id"),
                        "user_id": p1.get("user_id"),
                        "github_login": str(p1.get("github_login") or "").strip().lower(),
                        "sync_response_ms": elapsed_ms,
                    }

                    _e2e_log(
                        "e2e: sync enqueued",
                        token_fingerprint=token_fp,
                        ctx="incremental:first",
                        job_id=p1.get("job_id"),
                        user_id=p1.get("user_id"),
                        github_login=str(p1.get("github_login") or "").strip().lower(),
                        sync_response_ms=elapsed_ms,
                    )

                    w1 = _wait_for_job(
                        client,
                        p1["job_id"],
                        token_fingerprint=token_fp,
                        context="incremental:first",
                    )
                    token_summary["first_job"] = w1

                    start_t = time.time()
                    resp2 = client.post(
                        "/api/v1/sync",
                        json={"user_id": p1["user_id"], "github_token": token},
                    )
                    elapsed_ms = int((time.time() - start_t) * 1000)
                    assert resp2.status_code == 200, f"token={token_fp} {resp2.text}"
                    p2 = resp2.json()

                    token_summary["second_sync"] = {
                        "job_id": p2.get("job_id"),
                        "user_id": p2.get("user_id"),
                        "github_login": str(p2.get("github_login") or "").strip().lower(),
                        "sync_response_ms": elapsed_ms,
                    }

                    _e2e_log(
                        "e2e: sync enqueued",
                        token_fingerprint=token_fp,
                        ctx="incremental:second",
                        job_id=p2.get("job_id"),
                        user_id=p2.get("user_id"),
                        github_login=str(p2.get("github_login") or "").strip().lower(),
                        sync_response_ms=elapsed_ms,
                    )

                    w2 = _wait_for_job(
                        client,
                        p2["job_id"],
                        token_fingerprint=token_fp,
                        context="incremental:second",
                    )
                    token_summary["second_job"] = w2

                    backfill_1 = _get_sync_job_backfill_days(p1["job_id"])
                    backfill_2 = _get_sync_job_backfill_days(p2["job_id"])

                    token_summary["backfill_days"] = {
                        "first": backfill_1,
                        "second": backfill_2,
                    }

                    _e2e_log(
                        "e2e: backfill_days",
                        token_fingerprint=token_fp,
                        first_backfill_days=backfill_1,
                        second_backfill_days=backfill_2,
                    )

                    if backfill_1 is None:
                        raise AssertionError(
                            f"token={token_fp} expected first job backfill_days=1096 but got None; "
                            f"github_user_id={github_user_id}"
                        )

                    assert int(backfill_1) == 1096
                    assert backfill_2 is None

                    token_summary["status"] = "passed"
                except Exception as exc:
                    token_summary["status"] = "failed"
                    token_summary["error"] = str(exc)
                    raise
                finally:
                    summaries.append(token_summary)
                    _e2e_log("e2e: done token", token_fingerprint=token_fp)

                    if _e2e_log_enabled():
                        logger.info("e2e: token summary\n%s", json.dumps(token_summary, indent=2, sort_keys=True))
        finally:
            if _e2e_log_enabled():
                logger.info("e2e: summary incremental\n%s", json.dumps(summaries, indent=2, sort_keys=True))
