from services.worker.app.tasks import _derive_terminal_job_fields_from_pipeline_result


def test_derive_non_dict_result_returns_failed_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result(["not", "a", "dict"])
    assert status == "FAILED"
    assert "non-dict" in str(error_message or "").lower()


def test_derive_ok_status_returns_completed_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result({"status": "ok"})
    assert status == "COMPLETED"
    assert error_message is None


def test_derive_locked_status_returns_skipped_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result({"status": "locked"})
    assert status == "SKIPPED"
    assert "per-user lock" in str(error_message or "").lower()


def test_derive_missing_token_status_returns_failed_with_error_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result(
        {"status": "missing_token", "error": "Token vault not enabled"}
    )
    assert status == "FAILED"
    assert "token vault" in str(error_message or "").lower()


def test_derive_token_invalid_status_returns_failed_with_error_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result(
        {"status": "token_invalid", "error": "GitHub token unauthorized"}
    )
    assert status == "FAILED"
    assert "unauthorized" in str(error_message or "").lower()


def test_derive_failed_status_returns_failed_with_fallback_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result({"status": "failed"})
    assert status == "FAILED"
    assert "status=failed" in str(error_message or "").lower()


def test_derive_empty_status_returns_failed_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result({"status": "   "})
    assert status == "FAILED"
    assert "empty status" in str(error_message or "").lower()


def test_derive_unexpected_status_returns_failed_expected():
    status, error_message = _derive_terminal_job_fields_from_pipeline_result({"status": "weird"})
    assert status == "FAILED"
    assert "unexpected pipeline status=weird" in str(error_message or "").lower()

