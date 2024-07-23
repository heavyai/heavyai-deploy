CREATE FOREIGN TABLE IF NOT EXISTS XXTNAMEXX (
    longitude FLOAT,
    latitude FLOAT,
    hex06 TEXT ENCODING DICT(32),
    hex13 TEXT ENCODING DICT(32),
    hex13i BIGINT,
    huc12 TEXT ENCODING DICT(32),
    cbg_fips TEXT ENCODING DICT(32),
    state_fips TEXT ENCODING DICT(8),
    county_fips TEXT ENCODING DICT(16),
    model_ts TIMESTAMP(0) ENCODING FIXED(32),
    model_ts_str TEXT ENCODING DICT(32),
    forecast_ts TIMESTAMP(0) ENCODING FIXED(32),
    forecast_hour INTEGER,
    elevation_M FLOAT,
    elevation_Ft FLOAT,
    base_elevation FLOAT
)
SERVER XXSERVERXX
XXWITH_CLAUSEXX;