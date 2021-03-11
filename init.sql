CREATE TABLE test (
	timestamp NUMERIC,
	target_url TEXT,
	http_status INTEGER,
	latency_ms NUMERIC,
	error_text TEXT NULL,
	pattern TEXT NULL,
	is_pattern_found BOOLEAN NULL
);
