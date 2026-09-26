CREATE TABLE nas_identities
(
    user_id     UUID PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    external_id VARCHAR(255) NOT NULL UNIQUE,
    is_blocked  BOOLEAN      NOT NULL DEFAULT FALSE
);

INSERT INTO nas_identities (user_id, external_id, is_blocked)
SELECT sp.user_id, u.external_id, u.is_blocked
FROM storage_profiles sp
         JOIN users u ON u.id = sp.user_id
ON CONFLICT DO NOTHING;

ALTER TABLE storage_profiles
    ALTER COLUMN external_id DROP NOT NULL;
