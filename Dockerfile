FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY . .

RUN python -m pip install --upgrade pip && \
    if [ -f requirements.txt ]; then \
        python -m pip install -r requirements.txt; \
    elif [ -f pyproject.toml ]; then \
        python -m pip install .; \
    else \
        echo "No supported Python dependency manifest found" >&2; \
        exit 1; \
    fi

RUN groupadd --system appuser && \
    useradd --system --gid appuser --home-dir /app appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["sh", "-c", "python -m alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]