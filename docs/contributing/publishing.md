# Publishing Docs

## Local build and preview

- Install tools: `pip install -r docs/requirements.txt`
- Live preview: `mkdocs serve -f docs/mkdocs.yml`
- Build: `mkdocs build -f docs/mkdocs.yml`

## Versioning with mike

- Deploy a version and alias `latest`:
  - `mike deploy 0.1 latest`
- Add another version while keeping `latest` pointing to it:
  - `mike deploy 0.2 latest`
- Serve versions locally: `mike serve`
- List versions: `mike list`

## GitHub Pages (suggested)

1. Enable GitHub Pages on the `gh-pages` branch.
2. Publish from CI or locally using `mike deploy --push --update-aliases <version> latest`.
3. Optionally set `site_url` in `docs/mkdocs.yml` to the Pages URL for correct canonical links.

## CI checks

- Add a CI job to run `mkdocs build -f docs/mkdocs.yml` (see `.github/workflows/docs.yml`).
- Consider adding markdown linting and docstring checks to catch drift early.

