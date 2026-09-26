UPDATE storage_profiles sp
SET external_id = ni.external_id,
    is_blocked  = ni.is_blocked
FROM nas_identities ni
WHERE ni.user_id = sp.user_id
  AND sp.external_id IS NULL;

ALTER TABLE storage_profiles
    ALTER COLUMN external_id SET NOT NULL;

DROP TABLE nas_identities;
