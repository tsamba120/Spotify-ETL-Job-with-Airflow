/*
Temp Table Creation for a better view
*/
-- Function to create temp table
CREATE OR REPLACE FUNCTION create_temp_table() RETURNS void
LANGUAGE plpgsql
AS
$$
	BEGIN
	CREATE TABLE IF NOT EXISTS song_plays_detailed as (
		SELECT played_at, song_name, artist_name, album_name, duration_ms, popularity, release_date, song_url
		FROM spotify_schema.song_plays sp
			JOIN spotify_schema.dim_songs ds
				ON sp.song_id = ds.song_id
			JOIN spotify_schema.dim_artists da
				ON sp.artist_id = da.artist_id
			JOIN spotify_schema.dim_albums dalb
				ON ds.album_id = dalb.album_id
				AND da.artist_id = dalb.artist_id
		WHERE played_at::TIMESTAMP > (current_date - 7)
	);
END;
$$;
--DROP FUNCTION create_temp_table()

-- Trigger function to create table
SELECT create_temp_table();
select * from song_plays_detailed
order by played_at desc;
--DROP TABLE song_plays_detailed



/* 
Declare function to get Top 5 Songs by Listens
*/
CREATE OR REPLACE FUNCTION past_week_top_5_songs()
RETURNS TABLE (
	song_name TEXT, 
	artist_name TEXT, 
	listen_count INTEGER
)
LANGUAGE plpgsql
AS $$
	BEGIN
		RETURN query
			SELECT spd.song_name, spd.artist_name, COUNT(*)::INTEGER listen_count
			FROM song_plays_detailed spd
			GROUP BY 1,2
			ORDER BY COUNT(*) desc
			LIMIT 5;
END;
$$;

--DROP FUNCTION past_week_top_5_songs()
SELECT past_week_top_5_songs();



/* 
Declare function to get Top 5 Artists by Listens
*/
CREATE OR REPLACE FUNCTION past_week_top_5_artists()
RETURNS TABLE (
	artist_name TEXT,
	listen_count INTEGER
)
LANGUAGE plpgsql
AS $$
	BEGIN
		RETURN query
			select spd.artist_name, count(*)::INTEGER listen_count
			from song_plays_detailed spd
			group by 1
			order by count(*) desc
			limit 5;
END;
$$;
SELECT past_week_top_5_artists();



/* 
Declare function to get total listening time in minutes
*/
CREATE OR REPLACE FUNCTION get_listen_duration() 
RETURNS TABLE (total_time INTEGER) 
LANGUAGE plpgsql
AS $$
	BEGIN
		RETURN query
		select (SUM(spd.duration_ms)/1000/60)::INTEGER minutes_listened
		from song_plays_detailed spd;
END;
$$;

SELECT get_listen_duration();



/* 
Declare function to get Top 5 Most Popular Songs
*/
CREATE OR REPLACE FUNCTION most_popular_songs()
RETURNS TABLE (
	song_name TEXT,
	artist_name TEXT,
	popularity INTEGER
)
LANGUAGE plpgsql
AS $$
	BEGIN
	RETURN query
		SELECT DISTINCT spd.song_name, spd.artist_name, spd.popularity
		FROM song_plays_detailed spd
		ORDER BY popularity desc
		LIMIT 5;
	END;
$$;
SELECT most_popular_songs();



/* 
Declare function to get Top 5 Most Obscure Songs (at least 1 month old, so new "hits" are ignored)
*/
CREATE OR REPLACE FUNCTION most_obscure_songs()
RETURNS TABLE (
	song_name TEXT,
	artist_name TEXT,
	popularity INTEGER
)
LANGUAGE plpgsql
AS $$
	BEGIN
	RETURN query
		SELECT DISTINCT subq.song_name, subq.artist_name, subq.popularity
		FROM (
			SELECT *,
				CASE
					WHEN release_date = '2003' THEN '2003-01-01'
					ELSE release_date
				END cleaned_release_date
			FROM song_plays_detailed) subq
		WHERE cleaned_release_date::TIMESTAMP <= (current_date - 30)
		ORDER BY popularity ASC
		LIMIT 5;
END;
$$;
SELECT most_obscure_songs();



/* 
Declare function to get Top 5 Most Popular Albums
*/
CREATE OR REPLACE FUNCTION top_5_albums()
RETURNS TABLE (
	album_name TEXT,
	artist_name TEXT,
	listen_count INTEGER
)
LANGUAGE plpgsql
AS $$
	BEGIN
	RETURN query
		SELECT spd.album_name, spd.artist_name, COUNT(*)::INTEGER listen_count
		FROM song_plays_detailed spd
		GROUP BY 1,2
		ORDER BY count(*) desc
		LIMIT 5;
END;
$$;

SELECT top_5_albums();


-- Test functions
select create_temp_table();
select past_week_top_5_songs();
select past_week_top_5_artists();
select get_listen_duration();
select most_popular_songs();
select most_obscure_songs();
select top_5_albums();

-- Drop temp table from first function
DROP TABLE song_plays_detailed;
