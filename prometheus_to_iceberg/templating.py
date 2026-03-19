def resolve_variables(raw: dict | None) -> dict[str, str]:
    """Convert raw variable definitions to resolved string values.

    - str values pass through unchanged
    - list values are joined with '|' for PromQL regex alternation
    - Returns empty dict if input is None
    """
    if raw is None:
        return {}

    resolved = {}
    for key, value in raw.items():
        if isinstance(value, list):
            resolved[key] = "|".join(str(v) for v in value)
        else:
            resolved[key] = str(value)
    return resolved


def substitute(query: str, variables: dict[str, str]) -> str:
    """Replace $varname references in a query with resolved values.

    Keys are processed longest-first to prevent partial matches
    (e.g., $ns replacing part of $namespace).
    """
    for key in sorted(variables, key=len, reverse=True):
        query = query.replace(f"${key}", variables[key])
    return query
