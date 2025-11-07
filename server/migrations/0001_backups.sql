-- Migration number: 0001 	 2025-11-07T00:01:52.897Z

CREATE TABLE "backups" (
  id integer PRIMARY KEY,
  keeper_id text NOT NULL,
  checksum text NOT NULL,
  size integer NOT NULL,
  contact text NULL,
  contact_type text NULL,
  created_at text NOT NULL DEFAULT CURRENT_TIMESTAMP
);
