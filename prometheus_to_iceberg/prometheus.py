import logging
import time

import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 2


def query_range(
    base_url: str,
    query: str,
    start: float,
    end: float,
    step: str,
    timeout: int = 30,
) -> list[dict]:
    url = f"{base_url}/api/v1/query_range"
    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": step,
    }

    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            body = resp.json()

            if body.get("status") != "success":
                error_msg = body.get("error", "unknown error")
                raise ValueError(f"Prometheus query failed: {error_msg}")

            return body.get("data", {}).get("result", [])

        except requests.exceptions.HTTPError as e:
            if resp.status_code < 500:
                raise
            last_exc = e
        except requests.exceptions.ConnectionError as e:
            last_exc = e
        except requests.exceptions.Timeout as e:
            last_exc = e

        if attempt < MAX_RETRIES - 1:
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Prometheus request failed (attempt %d/%d), retrying in %ds: %s",
                attempt + 1,
                MAX_RETRIES,
                wait,
                last_exc,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Prometheus query failed after {MAX_RETRIES} attempts"
    ) from last_exc
