"""Separate displayed conditions from valid, physical performance-data pairs."""

from collections.abc import Callable, Iterable


CONDITION_SOURCE_CONTRACT = 'v4_primary_gate'


def fetch_enabled_fallbacks(fetch_all: Callable, fallbacks: dict[int, int]) -> dict[int, int]:
    """Enable fallback only for primaries with globally valid native data."""
    if not fallbacks:
        return {}
    params = {f'primary{i}': cid for i, cid in enumerate(sorted(fallbacks))}
    placeholders = ', '.join(f':{key}' for key in params)
    rows = fetch_all(
        f"""
            SELECT DISTINCT condition_id
            FROM mids_cids_in_data_view
            WHERE condition_id IN ({placeholders})
        """,
        params,
    )
    enabled = {int(row['condition_id']) for row in rows}
    return {primary: fallback for primary, fallback in fallbacks.items() if primary in enabled}


def select_sources(
    pairs: Iterable[tuple[int, int]],
    available: set[tuple[int, int]],
    fallbacks: dict[int, int],
) -> dict[tuple[int, int], int | None]:
    """Resolve one hop through enabled fallbacks; primary data takes precedence."""
    sources = {}
    for mid, primary in pairs:
        if (mid, primary) in available:
            sources[(mid, primary)] = primary
        else:
            fallback = fallbacks.get(primary)
            sources[(mid, primary)] = (
                fallback if fallback is not None and (mid, fallback) in available else None
            )
    return sources


def effective_pair_sql(model_column: str, condition_column: str, fallbacks: dict[int, int]) -> str:
    """SQL eligibility for a displayed pair. Column expressions must be code-owned."""
    alternatives = [f"source_pair.condition_id = {condition_column}"]
    for primary, fallback in sorted(fallbacks.items()):
        alternatives.append(
            f"({condition_column} = {int(primary)} AND source_pair.condition_id = {int(fallback)} "
            "AND EXISTS (SELECT 1 FROM mids_cids_in_data_view native_condition "
            f"WHERE native_condition.condition_id = {int(primary)}))"
        )
    return (
        "EXISTS (SELECT 1 FROM mids_cids_in_data_view source_pair "
        f"WHERE source_pair.model_id = {model_column} "
        f"AND ({' OR '.join(alternatives)}))"
    )
