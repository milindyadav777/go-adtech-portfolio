CREATE TABLE IF NOT EXISTS reward_events (
    publisher       text        NOT NULL,
    event_id        text        NOT NULL,
    user_id         text        NOT NULL,
    amount          bigint      NOT NULL CHECK (amount > 0),
    status          text        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts        bigint      NOT NULL DEFAULT 0,
    next_attempt_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    lease_until     timestamptz,
    last_error      text        NOT NULL DEFAULT '',
    PRIMARY KEY (publisher, event_id)
);

CREATE INDEX IF NOT EXISTS reward_events_due
    ON reward_events (next_attempt_at, publisher, event_id) WHERE status = 'pending';
