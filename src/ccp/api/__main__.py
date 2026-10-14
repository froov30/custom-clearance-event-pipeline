"""API service: python -m ccp.api (Swagger UI at http://127.0.0.1:8000/docs)."""

import uvicorn

from ccp.api.app import create_app
from ccp.common import mongo
from ccp.common.config import Config
from ccp.common.log import setup_logging


def main() -> None:
    cfg = Config.from_env()
    setup_logging("api", cfg.log_level)
    # log_config=None: uvicorn's own loggers propagate to our JSON handler instead of its format.
    uvicorn.run(
        create_app(mongo.database(cfg)), host=cfg.api_host, port=cfg.api_port, log_config=None
    )


if __name__ == "__main__":
    main()
