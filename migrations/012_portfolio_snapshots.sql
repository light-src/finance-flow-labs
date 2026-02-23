CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id BIGSERIAL PRIMARY KEY,
    as_of DATE NOT NULL UNIQUE,
    nav NUMERIC NOT NULL,
    us_weight NUMERIC,
    kr_weight NUMERIC,
    crypto_weight NUMERIC,
    leverage_weight NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT portfolio_snapshots_nav_positive CHECK (nav > 0),
    CONSTRAINT portfolio_snapshots_us_weight_range CHECK (
        us_weight IS NULL OR (us_weight >= 0 AND us_weight <= 1)
    ),
    CONSTRAINT portfolio_snapshots_kr_weight_range CHECK (
        kr_weight IS NULL OR (kr_weight >= 0 AND kr_weight <= 1)
    ),
    CONSTRAINT portfolio_snapshots_crypto_weight_range CHECK (
        crypto_weight IS NULL OR (crypto_weight >= 0 AND crypto_weight <= 1)
    ),
    CONSTRAINT portfolio_snapshots_leverage_weight_range CHECK (
        leverage_weight IS NULL OR (leverage_weight >= 0 AND leverage_weight <= 1)
    )
);

CREATE INDEX IF NOT EXISTS portfolio_snapshots_as_of_idx
    ON portfolio_snapshots (as_of DESC);
