/*
 * Because the input was so simple for this puzzle, I decided
 * to just use the text in the first CTE with dollar quoting. 
 */
/*
 * First Star
 * 
 */
with recursive input_lines AS (
	SELECT * FROM regexp_split_to_table($$Time:        54     70     82     75
Distance:   239   1142   1295   1253$$,'\n') WITH ORDINALITY x(lines,id)
),
races_param AS (
	SELECT id, o AS game, array_agg(g.val[1]) OVER(PARTITION BY o ORDER BY id) AS td --time_distance
		FROM input_lines, regexp_matches(lines, '(\d+)','g') WITH ORDINALITY g(val,o)
),
race_distance AS (
	SELECT game, td[1] time, td[2] dist, ms*(td[1]::int-ms) p_dist FROM races_param, generate_series(1,td[1]::int) AS ms WHERE id=2
),
total_wins AS (
	SELECT game, count(*) FILTER (WHERE dist::int < p_dist) FROM race_distance
	GROUP BY game
),
final_margin AS (
	SELECT game, count AS margin FROM total_wins WHERE game = 1
	UNION ALL 
	SELECT tw.game, fm.margin*tw.count AS margin FROM
		final_margin fm
	JOIN total_wins tw ON fm.game+1=tw.game
)
SELECT margin FROM final_margin ORDER BY game DESC LIMIT 1;


/*
 * Alternative, create an aggregate function to multiple
 * the values of each row, rather than use a resursive query
 */
CREATE FUNCTION row_multiplier (NUMERIC, NUMERIC, NUMERIC)
RETURNS NUMERIC AS
$$
	SELECT $1*$2;
$$ LANGUAGE 'sql' STRICT;

CREATE FUNCTION final_multiplier (NUMERIC)
RETURNS NUMERIC AS
$$
	SELECT $1;
$$ LANGUAGE 'sql' STRICT;

CREATE AGGREGATE multiply(numeric)
(
        INITCOND = 1,
        STYPE = numeric,
        SFUNC = row_multiplier
);

/*
 * Alternative First Star with custom aggregate function
 * 
 */
with recursive input_lines AS (
	SELECT * FROM regexp_split_to_table($$Time:        54     70     82     75
Distance:   239   1142   1295   1253$$,'\n') WITH ORDINALITY x(lines,id)
),
races_param AS (
	SELECT id, o AS game, array_agg(g.val[1]) OVER(PARTITION BY o ORDER BY id) AS td --time_distance
		FROM input_lines, regexp_matches(lines, '(\d+)','g') WITH ORDINALITY g(val,o)
),
race_distance AS (
	SELECT game, td[1] time, td[2] dist, ms*(td[1]::int-ms) p_dist FROM races_param, generate_series(1,td[1]::int) AS ms WHERE id=2
),
total_wins AS (
	SELECT game, count(*) FILTER (WHERE dist::int < p_dist) FROM race_distance
	GROUP BY game
)
SELECT multiply(count::numeric) FROM total_wins;


/*
 * Second Star
 * 
 */
with recursive input_lines AS (
	SELECT * FROM regexp_split_to_table($$Time:        54     70     82     75
Distance:   239   1142   1295   1253$$,'\n') WITH ORDINALITY x(lines,id)
),
races_param AS (
	SELECT max(x::bigint) FILTER (WHERE id=1) AS time,
		max(x::bigint) FILTER (WHERE id=2) AS dist
	FROM (
		SELECT id, string_agg(g.num[1],'') AS x
		FROM input_lines, regexp_matches(lines, '(\d+)','g') WITH ORDINALITY g(num,o)
		GROUP BY id
	) a
),
race_distance AS (
	SELECT i, dist, i*(time-i) p_dist FROM races_param, generate_series(dist/time,time) i
	ORDER BY i desc
)
--SELECT * FROM game_time;
SELECT count(*)
	FROM race_distance
		WHERE dist::bigint < p_dist;
