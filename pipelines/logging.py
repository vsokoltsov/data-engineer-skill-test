import logging
import os
import sys
import structlog

_CONFIGURED = False

def setup_logging(service_name: str) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return 
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        level=level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level, logging.INFO)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    structlog.get_logger().info("logger_initialized", service=service_name)
    _CONFIGURED = True