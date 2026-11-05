"""Features enabled only for verified, published immutable worker images."""

SERVICE = "VastRunner"

# Populate only after building, verifying and publishing the candidate image.
# An unpublished source change must never imply support in an existing image.
PUBLISHED_WORKER_CAPABILITIES: dict[str, frozenset[str]] = {}


def supports(image: str, feature: str) -> bool:
    """Return whether a published image explicitly supports this feature."""
    return feature in PUBLISHED_WORKER_CAPABILITIES.get(image, frozenset())


def maximum_rental_hours() -> float | None:
    """Expose the active published image's duration limit; None means budget-bound."""
    from vast_jobs import IMAGE
    return None if supports(IMAGE, 'multi_day') else 24
