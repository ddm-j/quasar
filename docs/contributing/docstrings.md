# Docstring Guide

This guide defines the required docstring style for the Quasar Python backend.
Use Google-style docstrings with explicit types in the docstring sections, even
when type hints are present in the signature.

## Standard

- Google style (`docstring_style: google` in `mkdocs.yml`).
- Summaries are imperative, one sentence, <= 80 characters.
- Include `Args`, `Returns`, and `Raises` sections where applicable; add `Yields`
  for generators and `Attributes` for classes.
- Include parameter and return types in the docstring, mirroring type hints.
- Describe side effects, external I/O, and noteworthy constraints.
- Keep examples short; prefer doctest-ready snippets when examples help clarity.

## Structure

```python
def func(arg1: str, *, flag: bool = False) -> list[str]:
    """Do the thing succinctly.

    Args:
        arg1 (str): Input description.
        flag (bool): Feature toggle description.

    Returns:
        list[str]: Result description.

    Raises:
        ValueError: When validation fails.
    """
```

### Classes
- Module-level docstring: purpose of the module.
- Class docstring: role of the class and important invariants.
- `Attributes` section for key instance attributes set in `__init__`.
- `__init__` docstring should document constructor parameters.

```python
class Client:
    """HTTP client for provider APIs.

    Attributes:
        timeout (float): Request timeout in seconds.
    """

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        """Create a client.

        Args:
            base_url (str): Provider base URL.
            timeout (float): Request timeout in seconds.
        """
```

### Async and generators
- Document coroutines the same way as sync functions.
- Use `Yields` instead of `Returns` for generators; include yield type.

```python
async def stream_ticks(symbol: str) -> AsyncIterator[Tick]:
    """Stream live ticks for a symbol.

    Args:
        symbol (str): Trading symbol, e.g., "BTC/USD".

    Yields:
        Tick: Next tick message.

    Raises:
        ProviderError: When the stream drops unexpectedly.
    """
```

## Dos and don'ts

- Do keep docstrings in sync with code (parameters, defaults, exceptions).
- Do explain units, expected shapes, and schema keys for dicts.
- Don't restate implementation details; focus on behavior and contract.
- Don't duplicate type information across multiple places inconsistently;
  ensure the docstring matches the signature.
