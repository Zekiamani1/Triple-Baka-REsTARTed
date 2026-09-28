CREATE TABLE clients (
    client_id       VARCHAR(20) PRIMARY KEY,
    client_secret   VARCHAR(100) NOT NULL,
    scope           VARCHAR(20) NOT NULL,
    display_name    VARCHAR(100),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE refresh_tokens (
    id          BIGSERIAL       PRIMARY KEY,
    client_id   VARCHAR(20)    NOT NULL REFERENCES clients(client_id),
    token_hash  TEXT            NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ     NOT NULL,
    revoked_at  TIMESTAMPTZ     NULL,
    issued_at   TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);