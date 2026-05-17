CREATE TABLE IF NOT EXISTS demand_weather_features (
	timestamp_utc TIMESTAMPTZ NOT NULL,
	region TEXT NOT NULL,

	demand_mwh DOUBLE PRECISION NOT NULL,
	temperature_2m DOUBLE PRECISION NOT NULL,

	hour SMALLINT NOT NULL,
	day_of_week SMALLINT NOT NULL,
	month SMALLINT NOT NULL,

	run_id TEXT NOT NULL,
	loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	PRIMARY KEY (region, timestamp_utc),

	CONSTRAINT demand_mwh_non_negative CHECK (demand_mwh >= 0),
	CONSTRAINT hour_valid CHECK (hour BETWEEN 0 AND 23),
    CONSTRAINT day_of_week_valid CHECK (day_of_week BETWEEN 0 AND 6),
    CONSTRAINT month_valid CHECK (month BETWEEN 1 AND 12)
);