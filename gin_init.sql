CREATE TABLE BOX (uid bigint,
                  lids integer[] NOT NULL
                  CHECK (array_ndims(lids) = 1));

CREATE OR REPLACE FUNCTION ulids(
    i_uid bigint,
    i_lids integer[]
) RETURNS bigint[] AS $$
    SELECT array_agg((i_uid << 32) | lid)
      FROM unnest(i_lids) lid;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE INDEX i_box_uid_lids
    ON box USING gin (ulids(uid, lids)) WITH (FASTUPDATE=OFF);
