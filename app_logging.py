import logging
from contextlib import contextmanager
from time import perf_counter


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)


@contextmanager
def log_stage(logger, stage, **fields):
    """Log a timed operation using caller-supplied, non-sensitive fields."""
    start = perf_counter()
    logger.info("stage=%s status=started %s", stage, _format_fields(fields))
    try:
        yield fields
    except Exception as error:
        logger.error(
            "stage=%s status=failed duration_seconds=%.3f error_type=%s %s",
            stage, perf_counter() - start, type(error).__name__, _format_fields(fields),
        )
        raise
    else:
        logger.info(
            "stage=%s status=completed duration_seconds=%.3f %s",
            stage, perf_counter() - start, _format_fields(fields),
        )


def _format_fields(fields):
    return " ".join(f"{key}={value}" for key, value in fields.items())


def run_command(main):
    configure_logging()
    try:
        main()
    except Exception:
        # The stage logged the error type. Avoid rendering raw exception details.
        raise SystemExit(1) from None
