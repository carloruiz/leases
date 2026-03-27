CREATE TABLE IF NOT EXISTS leases (
    resource_name STRING NOT NULL PRIMARY KEY,
    group_name    STRING NOT NULL,

    lease_token   UUID,
    claimed_by    STRING,
    claimed_at    TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    FAMILY default_f (resource_name, group_name, lease_token, claimed_by, claimed_at, created_at),
    FAMILY f_expires (expires_at),

    INDEX leases_claimable_by_group (group_name, expires_at, resource_name)
);
