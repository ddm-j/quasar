# API Reference

The reference section is generated from Python docstrings using `mkdocstrings`. Update docstrings when you change public behavior; regenerate by running `mkdocs build -f docs/mkdocs.yml`.

## Adding new modules

1. Create a Markdown file under `docs/reference/` with `::: package.module` blocks.
2. Link it from `nav` in `docs/mkdocs.yml`.
3. Keep docstrings consistent (see `contributing/docs-style.md`).

