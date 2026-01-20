"""DataHub constants and SQL queries."""

# Whether to pull data immediately upon subscription or wait for the next cron job
# (available for historical data providers ONLY)
IMMEDIATE_PULL = True

# Default number of seconds to offset the subscription cron job for live data providers
DEFAULT_LIVE_OFFSET = 30

# Default Number of bars to pull if we don't already have data
DEFAULT_LOOKBACK = 8000

# Number of bars to batch insert into the database
BATCH_SIZE = 500

# Allowed path for dynamic provider files
ALLOWED_DYNAMIC_PATH = '/app/dynamic_providers'

# SQL queries used by DataHub
QUERIES = {
    'get_subscriptions': """SELECT ps.provider, ps.interval, ps.cron,
                            array_agg(ps.sym ORDER BY ps.sym) AS syms,
                            array_agg(a.exchange ORDER BY ps.sym) AS exchanges
                            FROM provider_subscription ps
                            JOIN assets a ON (
                                ps.provider = a.class_name
                                AND ps.provider_class_type = a.class_type
                                AND ps.sym = a.symbol
                            )
                            GROUP BY ps.provider, ps.interval, ps.cron
                            """,
    'get_last_updated': """SELECT sym, last_updated::date AS d
                            FROM   historical_symbol_state
                            WHERE  provider = $1
                            AND  sym = ANY($2::text[])
                            """,
    'get_registered_provider': """SELECT file_path, file_hash, nonce, ciphertext, preferences, class_subtype
                                  FROM code_registry
                                  WHERE class_name = $1 AND class_type = 'provider';
                                  """,
    'get_index_providers_sync_config': """SELECT class_name,
                                                 COALESCE(preferences->'scheduling'->>'sync_frequency', '1w') AS sync_frequency
                                          FROM code_registry
                                          WHERE class_subtype = 'IndexProvider';
                                          """
}
