try:
    from .app import app
except Exception:  # pragma: no cover - optional dependency missing during tests
    app = None  # type: ignore[assignment]

__all__ = ['app']
