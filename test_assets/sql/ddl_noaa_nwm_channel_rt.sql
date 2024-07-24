CREATE FOREIGN TABLE IF NOT EXISTS XXTNAMEXX (
    model_ts TIMESTAMP(0) ENCODING FIXED(32),
    model_ts_str TEXT ENCODING DICT(32),
    forecast_ts TIMESTAMP(0) ENCODING FIXED(32),
    forecast_hour INTEGER,
    feature_id INTEGER,
    streamflow_M3PS FLOAT,
    streamflow_CFS FLOAT,
    nudge_M3PS FLOAT,
    nudge_CFS FLOAT,
    velocity_MPS FLOAT,
    velocity_FPS FLOAT,
    qSfcLatRunoff_M3PS FLOAT,
    qSfcLatRunoff_CFS FLOAT,
    qBucket_M3PS FLOAT,
    qBucket_CFS FLOAT,
    qBtmVertRunoff_M3 FLOAT,
    qBtmVertRunoff_CF FLOAT
)
SERVER XXSERVERXX
XXWITH_CLAUSEXX;