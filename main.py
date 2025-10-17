from __future__ import annotations

import asyncio
import logging

from crypto_hornet.config import Settings
from crypto_hornet.runner import run


def main() -> None:
    try:
        settings = Settings()
    except Exception as exc:  # noqa: BLE001
        logging.basicConfig(level=logging.INFO)
        logging.error("Failed to load configuration: %s", exc)
        raise
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()