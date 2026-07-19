from utils.logging import redact_sensitive_text


def test_redact_sensitive_query_values() -> None:
    text = (
        "GET https://example.test/private?timestamp=123"
        "&signature=abcdef&apiKey=visible"
    )
    redacted = redact_sensitive_text(text)
    assert "abcdef" not in redacted
    assert "visible" not in redacted
    assert "signature=<redacted>" in redacted
    assert "apiKey=<redacted>" in redacted


def test_redact_sensitive_json_values() -> None:
    redacted = redact_sensitive_text('{"token":"secret-value","code":10072}')
    assert "secret-value" not in redacted
    assert '"token":"<redacted>"' in redacted
