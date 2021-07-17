/*
Create artist dimension table
*/
CREATE TABLE IF NOT EXISTS spotify_schema.dim_artists(
	artist_id TEXT PRIMARY KEY NOT NULL,
	artist_name TEXT,
	artist_url TEXT
);

/*
Create album dimension table
*/
CREATE TABLE IF NOT EXISTS spotify_schema.dim_albums(
	album_id TEXT PRIMARY KEY NOT NULL,
	album_name TEXT,
	artist_id TEXT REFERENCES spotify_schema.dim_artists (artist_id),
	release_date TEXT,
	total_tracks INTEGER,
	album_url TEXT
);

/*
Create song dimension table
*/
CREATE TABLE IF NOT EXISTS spotify_schema.dim_songs(
	song_id TEXT PRIMARY KEY NOT NULL,
	song_name TEXT,
	artist_id TEXT REFERENCES spotify_schema.dim_artists (artist_id),
	album_id TEXT REFERENCES spotify_schema.dim_albums (album_id),
	duration_ms INTEGER,
	track_number INTEGER,
	popularity INTEGER,
	song_url TEXT
);

/* 
Create song plays table
*/
CREATE TABLE IF NOT EXISTS spotify_schema.song_plays(
	played_at TEXT PRIMARY KEY NOT NULL,
	song_id TEXT REFERENCES spotify_schema.dim_songs (song_id),
	artist_id TEXT REFERENCES spotify_schema.dim_artists (artist_id),
	timestamp TEXT
);