
CREATE TABLE IF NOT EXISTS NODES (
	node int PRIMARY KEY NOT NULL,
	geog geography(POINT,4326) NOT NULL,
	description text
);

CREATE TABLE IF NOT EXISTS WYCIECZKI (
	version int PRIMARY KEY NOT NULL,
	nodes_ref integer[] NOT NULL,
	type int DEFAULT 1 NOT NULL
	
);

CREATE TABLE IF NOT EXISTS CYCLISTS (
	cyclist text PRIMARY KEY NOT NULL,
	no_trips int,
	distance int
);

CREATE TABLE IF NOT EXISTS reservations (
	c_name text  NOT NULL, FOREIGN KEY (c_name) REFERENCES CYCLISTS (cyclist),
	s_date date  NOT NULL,
	wycieczka_version int NOT NULL,
	FOREIGN KEY (wycieczka_version) REFERENCES WYCIECZKI (version)
);

