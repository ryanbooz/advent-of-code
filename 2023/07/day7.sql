create table dec07 (
	id integer generated by default as identity,
	lines text
);

--\COPY dec07(lines) FROM input.txt NULL '';

/*
 * First Star
 * 
 */
--with input_lines AS (
--	SELECT * FROM regexp_split_to_table($$32T3K 765
--T55J5 684
--KK677 28
--KTJJT 220
--QQQJA 483$$,'\n') WITH ORDINALITY x(lines,id)
--),
--
-- Start by turning the hands and bid into a simple table for
-- further processing in the next CTE queries
WITH given_hand AS (
	SELECT id AS hand, g.*, (split_part(lines,' ',2))::bigint AS bid
	FROM dec07, regexp_split_to_table(split_part(lines,' ',1),'') WITH ORDINALITY g(card,o)
),
card_converter (card,value) AS ( 
	VALUES  ('2',2),
			('3',3),
			('4',4),
			('5',5),
			('6',6),
			('7',7),
			('8',8),
			('9',9),
			('T',10),
			('J',11),
			('Q',12),
			('K',13),
			('A',14)
),
-- Using COUNT(*) OVER(...) allows us to get the
-- count of each card type in a hand without using
-- a GROUP BY which reduces the number of rows in hands
-- where multiple of one card exists. This way, we retain
-- all of the cards for each hand along with the counts
-- to use later in the array_agg which will be used
-- to order the results for the final calculation.
card_counts AS (
	SELECT card, hand, bid, value, o, 
		count(*) over(PARTITION BY hand, card) FROM given_hand
		JOIN card_converter USING (card)
)
SELECT sum(bid*row_number) FROM (
	SELECT *, ROW_NUMBER() OVER() 
	FROM (
		SELECT hand, bid,
			array_agg(c1 ORDER BY c1 DESC, value desc) AS ordered_card_count,
			array_agg(value) AS card_values
		FROM (
			SELECT gh.hand, gh.card, gh.bid, cc.value, cc.count c1
			FROM given_hand gh
				JOIN card_counts cc USING (hand, o)
			ORDER BY hand, o
		) x
		GROUP BY hand, bid
		ORDER BY ordered_card_count, card_values
	) y
) z
;


/*
 * Second Star
 * 
 */
--with input_lines AS (
--	SELECT * FROM regexp_split_to_table($$32T3K 765
--T55J5 684
--KK677 28
--KTJJT 220
--QQQJA 483$$,'\n') WITH ORDINALITY x(lines,id)
--),

WITH card_converter (card,value) AS ( 
	VALUES  ('2',2),
			('3',3),
			('4',4),
			('5',5),
			('6',6),
			('7',7),
			('8',8),
			('9',9),
			('T',10),
			('J',1),
			('Q',12),
			('K',13),
			('A',14)
),
given_hand AS (
	SELECT id AS hand, g.*,	(split_part(lines,' ',2))::bigint AS bid
	FROM dec07, regexp_split_to_table(split_part(lines,' ',1),'') WITH ORDINALITY g(card,o)
),
card_counts AS (
	SELECT card, hand, bid, value, o,
		count(*) over(PARTITION BY hand, card) FROM given_hand
		JOIN card_converter USING (card)
),
-- now that we have the count of cards, we can
-- find the highest card value by total count of each
-- card. We need this to replace the J in our next step
-- to modify the original hand that was provided.
max_card AS (
	SELECT DISTINCT hand, max_value FROM card_counts cc
	JOIN LATERAL (
		SELECT value AS max_value FROM card_counts cc2
		WHERE cc2.hand = cc.hand AND value != 1
		ORDER BY count DESC, value DESC 
		LIMIT 1
	) mc ON true
),
-- Now we need to get a second set of "hands" where the Joker
-- value is replaced by the value of our "max_card" value for 
-- each hand. To order the hands correctly, we only need the card
-- values, not the actual face of the card. Originally I did output
-- both here just to verify the replacement, but no reason to add
-- more work and CASE statements if it's not needed.
modified_hand AS (
	SELECT hand, card, bid, o,
		CASE WHEN value = 1 THEN max_value ELSE value END new_value --(SELECT max_value FROM max_card WHERE given_hand.hand = max_card.hand) ELSE value END new_value
	FROM given_hand
	JOIN card_converter USING (card)
	LEFT JOIN max_card USING (hand)
),
-- Just like the original hand, we need to be able to order
-- the modified hand based on the new values for each hand
-- wherever a Joker was replaced.
modified_counts AS (
	SELECT card, hand, bid, new_value, o,
		count(*) over(PARTITION BY hand, new_value) FROM modified_hand
)
-- almost the same as the previous query, but now we will first
-- order based on the counts of the new hands and then the actual
-- values within the hand.
SELECT sum(bid*row_number) FROM (
	SELECT bid, ROW_NUMBER() OVER() FROM (
		SELECT hand, bid,
			array_agg(c2 ORDER BY c2 DESC, new_value desc) AS new_ordered_card_counts,
			array_agg(value) AS orig_card_values
		 FROM (
			SELECT gh.hand, gh.card, gh.bid, cc.value, cc.count c1,
				mh.new_value, mh.count c2
			FROM given_hand gh
				JOIN card_counts cc USING (hand, o)
				JOIN modified_counts mh USING (hand, o)
			ORDER BY hand, o
		) x
		GROUP BY hand, bid
		ORDER BY new_ordered_card_counts, orig_card_values
	) y
)z;