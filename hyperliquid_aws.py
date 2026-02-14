from __future__ import annotations

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional
from typing import Any, Callable
from threading import Lock

from hyperliquid_api import resolve_hyperliquid_coin_name
from market_data import normalize_market_data_coin_dir


HYPERLIQUID_AWS_REGION = "us-east-2"
HYPERLIQUID_AWS_BUCKET = "hyperliquid-archive"


def _normalize_archive_coin(coin: str) -> str:
    raw = str(coin or "").strip()
    if not raw:
        return ""

    base = raw.upper()
    if "/" in base:
        base = base.split("/", 1)[0]
    elif "_" in base and ":" in base:
        base = base.split("_", 1)[0]

    for suffix in ("USDC", "USDT", "USD"):
        if base.endswith(suffix) and len(base) > len(suffix):
            base = base[: -len(suffix)]
            break

    # Remove numeric multiplier prefix (1000PEPE → PEPE, 10000LADYS → LADYS)
    import re
    multiplier_match = re.match(r'^(1+0+)([A-Z]+)$', base)
    if multiplier_match:
        base = multiplier_match.group(2)

    try:
        resolved = resolve_hyperliquid_coin_name(coin=base, timeout_s=10.0)
    except Exception:
        resolved = ""

    if resolved:
        return resolved

    k_prefix_coins = {"BONK", "FLOKI", "LUNC", "PEPE", "SHIB", "DOGS", "NEIRO"}
    if base in k_prefix_coins:
        return f"k{base}"

    if base.startswith("K") and len(base) > 1 and base[1].isalpha():
        return f"k{base[1:]}"

    return base


def list_hyperliquid_archive_hours_aws(
    *,
    day: date | str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    timeout_s: float | None = None,
) -> list[str]:
    """List available hour prefixes for a given YYYYMMDD day.

    This is a lightweight probe to determine whether a day exists in the archive,
    without attempting downloads.

    Returns:
        Sorted list of hour folder names (e.g. ["00", "01", ...])
    """

    if isinstance(day, str):
        day_str = str(day).strip()
        if len(day_str) != 8 or not day_str.isdigit():
            raise ValueError("day must be YYYYMMDD")
    else:
        day_str = day.strftime("%Y%m%d")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    cfg = None
    if timeout_s is not None:
        try:
            from botocore.config import Config  # type: ignore

            cfg = Config(connect_timeout=timeout_s, read_timeout=timeout_s)
        except Exception:
            cfg = None

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=cfg,
    )

    prefix = f"market_data/{day_str}/"
    hours: set[str] = set()
    token: str | None = None
    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": "/",
            "RequestPayer": "requester",
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token

        resp = client.list_objects_v2(**kwargs)
        for cp in resp.get("CommonPrefixes", []) or []:
            pfx = str(cp.get("Prefix") or "")
            # pfx looks like: market_data/YYYYMMDD/HH/
            rest = pfx[len(prefix) :]
            hour = rest.strip("/").split("/", 1)[0]
            if hour:
                hours.add(hour)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break

    def _sort_key(h: str) -> int:
        try:
            return int(h)
        except Exception:
            return 999

    return sorted(hours, key=_sort_key)


def check_hyperliquid_l2book_coin_exists_aws(
    *,
    coin: str,
    day: date | str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    dataset: str = "l2Book",
    hours: Iterable[str] | None = None,
    sample_hours: int = 3,
) -> bool:
    """Probe whether at least one l2Book object exists for a coin on a day.

    Uses a small sample of hours to keep the check fast.
    """

    coin_u = _normalize_archive_coin(coin)
    if not coin_u:
        return False

    if isinstance(day, str):
        day_str = str(day).strip()
        if len(day_str) != 8 or not day_str.isdigit():
            raise ValueError("day must be YYYYMMDD")
    else:
        day_str = day.strftime("%Y%m%d")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    hours_list = [str(h).strip() for h in (hours or []) if str(h).strip()]
    if not hours_list:
        hours_list = list_hyperliquid_archive_hours_aws(
            day=day_str,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            bucket=bucket,
        )

    if not hours_list:
        return False

    # Choose a small sample of hours (first/middle/last) for a quick probe.
    sample: list[str] = []
    if len(hours_list) <= max(1, int(sample_hours)):
        sample = hours_list
    else:
        mid = len(hours_list) // 2
        sample = [hours_list[0], hours_list[mid], hours_list[-1]]

    seen = set()
    for hour_str in sample:
        if hour_str in seen:
            continue
        seen.add(hour_str)
        key_prefix = f"market_data/{day_str}/{hour_str}/{dataset}/{coin_u}.lz4"
        resp = client.list_objects_v2(
            Bucket=bucket,
            Prefix=key_prefix,
            MaxKeys=1,
            RequestPayer="requester",
        )
        if resp.get("KeyCount") or resp.get("Contents"):
            return True

    return False


def list_hyperliquid_archive_days_aws(
    *,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    limit: int = 200,
    max_pages: int = 50,
) -> list[str]:
    """List available day prefixes under market_data/.

    Returns:
        Sorted list of day folder names (YYYYMMDD) up to `limit`.
    """

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    prefix = "market_data/"
    days: set[str] = set()
    token: str | None = None
    pages = 0
    while True:
        pages += 1
        if max_pages and pages > int(max_pages):
            break
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": "/",
            "RequestPayer": "requester",
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for cp in resp.get("CommonPrefixes", []) or []:
            pfx = str(cp.get("Prefix") or "")
            rest = pfx[len(prefix) :]
            day = rest.strip("/").split("/", 1)[0]
            if len(day) == 8 and day.isdigit():
                days.add(day)
                if limit and len(days) >= int(limit):
                    break
        if limit and len(days) >= int(limit):
            break
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break

    return sorted(days)


def get_hyperliquid_archive_latest_day_aws(
    *,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    max_pages: int = 200,
) -> str:
    """Return the latest (max) YYYYMMDD day prefix under market_data/.

    Uses pagination and tracks the maximum day without storing all prefixes.
    """

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    prefix = "market_data/"
    token: str | None = None
    pages = 0
    latest = ""

    while True:
        pages += 1
        if max_pages and pages > int(max_pages):
            break

        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": "/",
            "RequestPayer": "requester",
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token

        resp = client.list_objects_v2(**kwargs)
        for cp in resp.get("CommonPrefixes", []) or []:
            pfx = str(cp.get("Prefix") or "")
            rest = pfx[len(prefix) :]
            day = rest.strip("/").split("/", 1)[0]
            if len(day) == 8 and day.isdigit():
                if day > latest:
                    latest = day

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break

    return latest


def get_hyperliquid_archive_day_range_aws(
    *,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    max_pages: int = 200,
) -> tuple[str, str]:
    """Return (oldest_day, newest_day) as YYYYMMDD strings.

    Scans day prefixes under market_data/ and tracks both min and max.
    """

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    prefix = "market_data/"
    token: str | None = None
    pages = 0
    oldest = ""
    newest = ""

    while True:
        pages += 1
        if max_pages and pages > int(max_pages):
            break

        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": "/",
            "RequestPayer": "requester",
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token

        resp = client.list_objects_v2(**kwargs)
        for cp in resp.get("CommonPrefixes", []) or []:
            pfx = str(cp.get("Prefix") or "")
            rest = pfx[len(prefix) :]
            day = rest.strip("/").split("/", 1)[0]
            if len(day) == 8 and day.isdigit():
                if not oldest or day < oldest:
                    oldest = day
                if day > newest:
                    newest = day

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break

    return oldest, newest


def list_hyperliquid_archive_sample_keys_aws(
    *,
    prefix: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    max_keys: int = 20,
) -> list[str]:
    """Return up to `max_keys` object keys under a given prefix."""

    pfx = str(prefix or "").strip()
    if not pfx:
        raise ValueError("prefix is empty")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    resp = client.list_objects_v2(
        Bucket=bucket,
        Prefix=pfx,
        RequestPayer="requester",
        MaxKeys=int(max_keys),
    )
    out: list[str] = []
    for obj in resp.get("Contents", []) or []:
        k = str(obj.get("Key") or "")
        if k:
            out.append(k)
    return out


# --- Node fills (trades) helper ----------------------------------------------
HYPERLIQUID_NODE_FILLS_BUCKET = "hl-mainnet-node-data"


def list_hyperliquid_node_fills_day_keys(
    *,
    day: date | str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_NODE_FILLS_BUCKET,
    max_keys: int = 1000,
) -> list[str]:
    """List object keys under node_fills/YYYYMMDD/ for a given day.

    Returns a list of object keys (may include subfolders). Raises on missing creds.
    """

    if isinstance(day, str):
        day_str = str(day).strip()
        if len(day_str) != 8 or not day_str.isdigit():
            raise ValueError("day must be YYYYMMDD")
    else:
        day_str = day.strftime("%Y%m%d")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    prefix = f"node_fills/{day_str}/"
    out: list[str] = []
    token: str | None = None
    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "RequestPayer": "requester",
            "MaxKeys": int(max_keys),
        }
        if token:
            kwargs["ContinuationToken"] = token

        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            k = str(obj.get("Key") or "")
            if k:
                out.append(k)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break

    return out


def get_default_hyperliquid_raw_dir() -> Path:
    """Default raw-data directory inside PBGui.

    User requirement: keep the raw AWS objects under:
        pbgui/data/ohlcv/hyperliquid
    """

    return (Path(__file__).resolve().parent / "data" / "ohlcv" / "hyperliquid").resolve()


@dataclass(frozen=True)
class HyperliquidS3Object:
    key: str
    out_path: Path


def _parse_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _iter_dates_inclusive(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_hyperliquid_l2book_s3_objects(
    coin: str,
    start_date: date | str,
    end_date: date | str,
    out_dir: str | Path | None = None,
    *,
    dataset: str = "l2Book",
    hours: Iterable[int] | None = None,
) -> list[HyperliquidS3Object]:
    """Build the S3 keys + output paths for Hyperliquid hourly l2Book archives.

    The public requester-pays S3 layout (as used by common community scripts) is:
        market_data/YYYYMMDD/H/l2Book/COIN.lz4

    Args:
        coin: Asset symbol, e.g. "BTC" or "BTC_USDC:USDC".
        start_date: `date` or YYYYMMDD string.
        end_date: `date` or YYYYMMDD string.
        out_dir: Base output directory.

    Returns:
        List of objects to download.
    """

    coin_input = str(coin or "").strip()
    if not coin_input:
        raise ValueError("coin is empty")
    
    # Normalize for S3 keys (kBONK, kPEPE, etc.) - archive format
    coin_normalized = _normalize_archive_coin(coin_input)
    if not coin_normalized:
        raise ValueError("coin is empty or invalid")
    
    # Normalize for local directory (BTC_USDC:USDC, etc.) - same as 1m_api
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin_input)
    if not coin_dir:
        raise ValueError(f"Failed to normalize coin directory for '{coin_input}'")

    if isinstance(start_date, str):
        start = _parse_yyyymmdd(start_date)
    else:
        start = start_date

    if isinstance(end_date, str):
        end = _parse_yyyymmdd(end_date)
    else:
        end = end_date

    base_out_dir = get_default_hyperliquid_raw_dir() if out_dir is None else Path(out_dir)
    dataset_dir = str(dataset or "").strip() or "l2Book"
    # Use normalized directory name (e.g., BTC_USDC:USDC, BONK_USDC:USDC)
    # Same as 1m_api for consistency
    base = base_out_dir.expanduser().resolve() / dataset_dir / coin_dir

    if hours is None:
        hours_list = list(range(24))
    else:
        hours_list = []
        for h in hours:
            try:
                ih = int(h)
            except Exception:
                continue
            if 0 <= ih <= 23:
                hours_list.append(ih)
        hours_list = sorted(set(hours_list))
        if not hours_list:
            raise ValueError("hours is empty (must contain values 0..23)")

    objects: list[HyperliquidS3Object] = []
    for day in _iter_dates_inclusive(start, end):
        day_str = day.strftime("%Y%m%d")
        for hour in hours_list:
            hour_str = f"{int(hour):02d}"
            key = f"market_data/{day_str}/{hour_str}/{dataset_dir}/{coin_normalized}.lz4"
            out_path = base / f"{day_str}-{hour_str}.lz4"
            objects.append(HyperliquidS3Object(key=key, out_path=out_path))
    return objects


def download_hyperliquid_l2book_aws(
    *,
    coin: str,
    start_date: date | str,
    end_date: date | str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    out_dir: str | Path | None = None,
    region_name: str = HYPERLIQUID_AWS_REGION,
    bucket: str = HYPERLIQUID_AWS_BUCKET,
    max_workers: int = 8,
    overwrite: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    collect_failed_keys: bool = False,
    dataset: str = "l2Book",
    hours: Iterable[int] | None = None,
    fail_fast: bool = False,
    recent_keys_limit: int | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Download Hyperliquid hourly `l2Book` .lz4 files from AWS S3.

    Notes:
    - This bucket uses requester-pays; you are billed for the request + data.
    - Credentials are provided by the caller (no hardcoding).

    Returns:
        Dict with keys: `planned`, `skipped_existing`, `downloaded`, `failed`.
    """

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are required")

    planned_objects = build_hyperliquid_l2book_s3_objects(
        coin=coin,
        start_date=start_date,
        end_date=end_date,
        out_dir=out_dir,
        dataset=dataset,
        hours=hours,
    )

    if dry_run:
        return {
            "planned": len(planned_objects),
            "skipped_existing": 0,
            "downloaded": 0,
            "failed": 0,
        }

    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'boto3'. Install it in the PBGui environment: pip install boto3"
        ) from exc

    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Create a single boto3 client to reuse connections across all threads.
    # boto3 clients are thread-safe and share connection pools.
    s3_client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    def _s3_error_code(exc: Exception) -> str:
        try:
            # botocore.exceptions.ClientError
            resp = getattr(exc, "response", None)
            if isinstance(resp, dict):
                err = resp.get("Error")
                if isinstance(err, dict):
                    return str(err.get("Code") or "")
        except Exception:
            pass
        return ""

    def _alt_key_non_padded_hour(key: str) -> str | None:
        parts = str(key or "").split("/")
        # expected: market_data/YYYYMMDD/HH/dataset/COIN.lz4
        if len(parts) < 5:
            return None
        if parts[0] != "market_data":
            return None
        hour_part = parts[2]
        if not hour_part.isdigit():
            return None
        # if already non-padded, nothing to do
        if len(hour_part) == 1:
            return None
        # convert "00" -> "0" etc.
        try:
            parts[2] = str(int(hour_part))
        except Exception:
            return None
        return "/".join(parts)

    def _stream_download(client, key: str, out_path: Path) -> int:
        response = client.get_object(Bucket=bucket, Key=key, RequestPayer="requester")
        total_bytes = int(response.get("ContentLength") or 0)
        body = response["Body"]

        tmp_path = out_path.with_suffix(out_path.suffix + ".part")
        tmp_path.parent.mkdir(parents=True, exist_ok=True)

        read_bytes = 0
        with active_lock:
            active_map[key] = {
                "key": key,
                "status": "downloading",
                "bytes_read": int(read_bytes),
                "bytes_total": int(total_bytes),
            }

        with open(tmp_path, "wb") as f:
            while True:
                chunk = body.read(512 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                read_bytes += len(chunk)
                with active_lock:
                    active = active_map.get(key)
                    if isinstance(active, dict):
                        active["bytes_read"] = int(read_bytes)
                        active["bytes_total"] = int(total_bytes)

                if progress_cb:
                    try:
                        with active_lock:
                            active_list = list(active_map.values())
                        progress_cb(
                            {
                                "planned": len(planned_objects),
                                "done": int(downloaded + skipped_existing + failed),
                                "downloaded": int(downloaded),
                                "skipped_existing": int(skipped_existing),
                                "failed": int(failed),
                                "downloaded_bytes": int(downloaded_bytes),
                                "skipped_existing_bytes": int(skipped_existing_bytes),
                                "failed_bytes": int(failed_bytes),
                                "recent_keys": list(recent_keys),
                                "active_downloads": active_list,
                            }
                        )
                    except Exception:
                        pass

        os.replace(tmp_path, out_path)
        with active_lock:
            active_map.pop(key, None)
        return int(read_bytes)

    def download_one(obj: HyperliquidS3Object) -> tuple[bool, bool, str, int]:
        """Returns (downloaded, failed, key, nbytes)."""

        obj.out_path.parent.mkdir(parents=True, exist_ok=True)

        if obj.out_path.exists() and not overwrite:
            try:
                return (False, False, obj.key, int(obj.out_path.stat().st_size))
            except Exception:
                return (False, False, obj.key, 0)

        try:
            read_bytes = _stream_download(s3_client, obj.key, obj.out_path)
            return (True, False, obj.key, int(read_bytes))
        except Exception as exc:
            # Some archive layouts use hour folders without zero padding.
            if _s3_error_code(exc) == "NoSuchKey":
                alt_key = _alt_key_non_padded_hour(obj.key)
                if alt_key:
                    try:
                        read_bytes = _stream_download(s3_client, alt_key, obj.out_path)
                        return (True, False, obj.key, int(read_bytes))
                    except Exception:
                        pass

            if fail_fast:
                raise
            if verbose:
                print(f"[hyperliquid_aws] download failed: s3://{bucket}/{obj.key}: {exc!r}")
            err_code = _s3_error_code(exc)
            err_msg = f"{err_code}: {exc}" if err_code else str(exc)
            error_by_key[obj.key] = err_msg
            with active_lock:
                active_map.pop(obj.key, None)
            return (False, True, obj.key, 0)

    skipped_existing = 0
    downloaded = 0
    failed = 0
    skipped_existing_bytes = 0
    downloaded_bytes = 0
    failed_bytes = 0
    failed_keys: list[str] = []
    recent_keys: list[dict[str, Any]] = []
    recent_failed: list[dict[str, Any]] = []
    active_lock = Lock()
    active_map: dict[str, dict[str, Any]] = {}
    error_by_key: dict[str, str] = {}

    if max_workers < 1:
        raise ValueError("max_workers must be >= 1")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_one, obj): obj for obj in planned_objects}
        for fut in as_completed(futures):
            did_download, did_fail, obj_key, nbytes = fut.result()
            if did_fail:
                failed += 1
                failed_bytes += int(nbytes or 0)
                if collect_failed_keys and obj_key:
                    failed_keys.append(obj_key)
                if recent_keys_limit and obj_key:
                    recent_keys.append(
                        {
                            "key": obj_key,
                            "status": "failed",
                            "bytes": int(nbytes or 0),
                            "error": error_by_key.get(obj_key) or "",
                        }
                    )
                if recent_keys_limit and obj_key:
                    recent_failed.append(
                        {
                            "key": obj_key,
                            "bytes": int(nbytes or 0),
                            "error": error_by_key.get(obj_key) or "",
                        }
                    )
            elif did_download:
                downloaded += 1
                downloaded_bytes += int(nbytes or 0)
                if recent_keys_limit and obj_key:
                    recent_keys.append({"key": obj_key, "status": "downloaded", "bytes": int(nbytes or 0)})
            else:
                skipped_existing += 1
                skipped_existing_bytes += int(nbytes or 0)

            if recent_keys_limit and len(recent_keys) > int(recent_keys_limit):
                recent_keys = recent_keys[-int(recent_keys_limit):]
            if recent_keys_limit and len(recent_failed) > int(recent_keys_limit):
                recent_failed = recent_failed[-int(recent_keys_limit):]

            if progress_cb:
                try:
                    with active_lock:
                        active_list = list(active_map.values())
                    progress_cb(
                        {
                            "planned": len(planned_objects),
                            "done": int(downloaded + skipped_existing + failed),
                            "downloaded": int(downloaded),
                            "skipped_existing": int(skipped_existing),
                            "failed": int(failed),
                            "downloaded_bytes": int(downloaded_bytes),
                            "skipped_existing_bytes": int(skipped_existing_bytes),
                            "failed_bytes": int(failed_bytes),
                            "recent_keys": list(recent_keys),
                            "recent_failed": list(recent_failed),
                            "active_downloads": active_list,
                        }
                    )
                except Exception:
                    pass

    result: dict[str, Any] = {
        "planned": len(planned_objects),
        "skipped_existing": skipped_existing,
        "downloaded": downloaded,
        "failed": failed,
        "skipped_existing_bytes": skipped_existing_bytes,
        "downloaded_bytes": downloaded_bytes,
        "failed_bytes": failed_bytes,
        "total_bytes": int(skipped_existing_bytes + downloaded_bytes + failed_bytes),
        "max_workers": max_workers,
    }
    if collect_failed_keys and failed_keys:
        # Keep it non-breaking: only include this key when explicitly requested.
        result["failed_keys_count"] = len(failed_keys)
    if recent_keys_limit:
        result["recent_keys"] = recent_keys  # type: ignore[assignment]
        result["recent_failed"] = recent_failed  # type: ignore[assignment]
    return result
