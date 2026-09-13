"""Entry point: ``python -m rad.generator`` (configured with RAD_GEN_* environment variables)."""

import asyncio
import contextlib
import logging

from rad.generator.runner import GeneratorSettings, run


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # httpx logs every request at INFO: several lines per second that bury the stats line.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(GeneratorSettings()))


if __name__ == "__main__":
    main()
