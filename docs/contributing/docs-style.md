# Documentation and Docstring Style

## Docstrings

- Standard: Google style docstrings (works well with `mkdocstrings`); see `contributing/docstrings.md` for the full template.
- Include type hints in signatures and repeat the types in `Args`/`Returns`/`Raises` sections.
- Keep public interfaces documented; private helpers can stay minimal.

Example:

```python
def fetch_symbols(provider: str) -> list[str]:
    """Fetch tradable symbols for a provider.

    Args:
        provider: Provider identifier, e.g., \"kraken\".

    Returns:
        A list of tradable symbol strings.

    Raises:
        ProviderError: If the provider call fails.
    """
```

If you prefer NumPy or reStructuredText style, switch `docstring_style` in `docs/mkdocs.yml` and keep it consistent.

## Markdown docs

- Use short sections, headings, and task lists where helpful.
- Prefer diagrams-as-code with Mermaid for architecture or flows.
- Link to code (`quasar/lib/...`) instead of duplicating snippets when possible.

## Validation

- Run `mkdocs build -f docs/mkdocs.yml` before merging.
- Optional: enable `ruff`/`pydocstyle` docstring checks and markdown linting (e.g., `remark-lint`) in CI.

