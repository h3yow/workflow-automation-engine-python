"""Step handler registry and per-step execution with retry/backoff."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .models import RetryPolicy, StepResult

logger = logging.getLogger("workflow_engine")

StepHandler = Callable[[], object]


class RetryableStepError(Exception):
    """Handlers raise this to signal a recoverable failure worth retrying.

    Any other exception is treated as an unrecoverable failure: the step
    terminates immediately (its configured retries are not consumed).
    """


class StepExecutor:
    def __init__(self, sleep: Callable[[float], None] | None = None) -> None:
        self._handlers: dict[str, StepHandler] = {}
        self._sleep = sleep if sleep is not None else time.sleep

    def register(self, name: str, handler: StepHandler) -> None:
        self._handlers[name] = handler

    def execute(
        self,
        handler_name: str,
        retry_policy: RetryPolicy,
        run_id: str,
        step_id: str,
    ) -> StepResult:
        """Run one step with bounded exponential-backoff retries.

        Retry decision basis (logged with run_id + step_id):

        * success on the first normal return;
        * ``RetryableStepError`` -> wait ``delay_for_attempt(n+1)`` and retry,
          up to ``max_attempts`` total attempts;
        * any other exception -> terminal failure immediately;
        * retryable errors after the final attempt -> terminal failure.
        """
        handler = self._handlers.get(handler_name)
        if handler is None:
            logger.warning(
                "run_id=%s step_id=%s basis=handler_not_registered handler=%s -> failed(non-retryable)",
                run_id, step_id, handler_name,
            )
            return StepResult(success=False, retryable=False, attempts=0,
                              error=f"handler {handler_name!r} is not registered")

        attempts = 0
        while attempts < retry_policy.max_attempts:
            attempts += 1
            try:
                output = handler()
            except RetryableStepError as exc:
                error = str(exc) or exc.__class__.__name__
                if attempts >= retry_policy.max_attempts:
                    logger.warning(
                        "run_id=%s step_id=%s attempt=%d basis=retryable_failure "
                        "error=%r retries_exhausted=%d -> failed(terminal)",
                        run_id, step_id, attempts, error, retry_policy.max_attempts,
                    )
                    return StepResult(success=False, retryable=True, error=error, attempts=attempts)
                delay = retry_policy.delay_for_attempt(attempts + 1)
                logger.info(
                    "run_id=%s step_id=%s attempt=%d basis=retryable_failure error=%r "
                    "-> backoff=%.3fs then retry",
                    run_id, step_id, attempts, error, delay,
                )
                if delay > 0:
                    self._sleep(delay)
                continue
            except Exception as exc:  # noqa: BLE001 - every other failure is terminal
                error = f"{exc.__class__.__name__}: {exc}"
                logger.warning(
                    "run_id=%s step_id=%s attempt=%d basis=non_retryable_error error=%r "
                    "-> failed(terminal, no retry)",
                    run_id, step_id, attempts, error,
                )
                return StepResult(success=False, retryable=False, error=error, attempts=attempts)

            logger.info(
                "run_id=%s step_id=%s attempt=%d basis=handler_returned -> succeeded",
                run_id, step_id, attempts,
            )
            return StepResult(success=True, output=output, attempts=attempts)

        # Defensive: loop only exits here when max_attempts < 1 (rejected at validation).
        return StepResult(success=False, retryable=True, error="retry policy exhausted", attempts=attempts)
