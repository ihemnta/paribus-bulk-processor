"""Run database migrations, then start the FastAPI server."""

import os
import subprocess
import sys


def main() -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
    )

    os.execv(
        sys.executable,
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            os.environ.get("PORT", "10000"),
        ],
    )


if __name__ == "__main__":
    main()