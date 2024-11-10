--top5 outgoing tvp usd ranking grouped by week and vertical 
WITH ranking AS (
	SELECT 
		*,
		RANK() OVER (PARTITION BY week ORDER BY outgoing_tvp_usd DESC) AS ranking
	FROM my_user.safe_tvp_vertical)
SELECT 
	*
FROM ranking 
	WHERE ranking < 6;


--top5 total transaction ranking grouped by week and vertical
WITH ranking AS (
	SELECT 
		*,
		RANK() OVER (PARTITION BY week ORDER BY total_transactions DESC) AS ranking
	FROM my_user.safe_tvp_vertical)
SELECT 
	*
FROM ranking 
	WHERE ranking < 6;