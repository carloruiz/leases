CREATE TABLE IF NOT EXISTS leases (
    resource_name TEXT NOT NULL PRIMARY KEY,
    group_name    TEXT NOT NULL,

    lease_token   UUID,
    claimed_by    TEXT,
    claimed_at    TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS leases_claimable_by_group
    ON leases (group_name, expires_at, resource_name);
