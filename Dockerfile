# --- build stage --------------------------------------------------
FROM python:3.12-slim AS base
WORKDIR /app

# copy project code
COPY pyproject.toml .
COPY quasar/ quasar/

# editable install inside image
RUN pip install --no-cache-dir -e .[dev]

# final image
FROM python:3.12-slim
WORKDIR /app
COPY --from=base /usr/local /usr/local
COPY quasar/ quasar/
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "quasar.datahub_app"]
    