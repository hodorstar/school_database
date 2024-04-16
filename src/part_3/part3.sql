-- =========================01=============================

DROP FUNCTION IF EXISTS get_transferred_points_summary();
CREATE OR REPLACE FUNCTION get_transferred_points_summary()
RETURNS TABLE (peer_1 VARCHAR, peer_2 VARCHAR, points_amount BIGINT)
AS $$
BEGIN
    RETURN QUERY
    WITH paired_points AS (
    SELECT tp1."CheckingPeer" AS peer1_name,
            tp1."CheckedPeer" AS peer2_name,
            SUM(tp1."PointsAmount"  - COALESCE(tp2."PointsAmount" , 0)) AS points
        FROM "TransferredPoints" AS tp1
            LEFT JOIN "TransferredPoints" AS tp2
            ON tp1."CheckedPeer" = tp2."CheckingPeer"
            AND tp1."CheckingPeer" = tp2."CheckedPeer"
        GROUP BY tp1."CheckingPeer", tp1."CheckedPeer"
        )
    SELECT ranked_points.peer1_name,
        ranked_points.peer2_name,
        ranked_points.points
    FROM (
        SELECT peer1_name,
                peer2_name,
                points,
                ROW_NUMBER() OVER(PARTITION BY
                    LEAST(peer1_name, peer2_name),
                    GREATEST(peer1_name, peer2_name)
                    ORDER BY peer1_name, peer2_name) AS rn
            FROM paired_points
    ) ranked_points
WHERE rn = 1;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM get_transferred_points_summary()
-- Where peer_2 = 'mvazvelhwy';


-- =========================02=============================

CREATE OR REPLACE FUNCTION fnc_completed_tasks()
RETURNS TABLE("Peer" VARCHAR, "Task" VARCHAR, "XP" INTEGER) AS $$
BEGIN
	RETURN QUERY
	SELECT "Checks"."Peer" AS "Peer", "Checks"."Task" AS "Task", "XP"."XPAmount" AS "XP"
	FROM "Checks"
	JOIN "XP" ON "Checks"."ID" = "XP"."Check";
END;
$$ LANGUAGE plpgsql;


-- =========================03=============================

CREATE OR REPLACE FUNCTION fnc_peers_not_left_campus(Day DATE)
RETURNS TABLE("Peer" VARCHAR) AS $$
BEGIN
	RETURN QUERY
	SELECT  "TimeTracking"."Peer" AS "Peer"
	FROM "TimeTracking"
	WHERE "TimeTracking"."Date" = Day
	GROUP BY "TimeTracking"."Peer"
	HAVING SUM("State") = 1
	ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

-- =========================04=============================



DROP FUNCTION IF EXISTS calculate_peer_points_change();
CREATE OR REPLACE FUNCTION calculate_peer_points_change()
RETURNS TABLE (peer VARCHAR, points_change NUMERIC)
AS $$
BEGIN
    RETURN QUERY
    WITH transfered AS (
    SELECT "CheckingPeer" AS peer_name,
        SUM("PointsAmount") AS points_change
    FROM "TransferredPoints"
    GROUP BY "CheckingPeer"
    UNION ALL
    SELECT "CheckedPeer" AS peer_name,
        -SUM("PointsAmount") AS points_change
    FROM "TransferredPoints"
    GROUP BY "CheckedPeer"
    )
    SELECT peer_name,
        SUM(t.points_change)
    FROM transfered AS t
    GROUP BY peer_name
    ORDER BY SUM(t.points_change) DESC, peer_name;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM calculate_peer_points_change();

-- SELECT * FROM calculate_peer_points_change();



-- =========================05=============================


DROP FUNCTION IF EXISTS calculate_peer_points_change_2();
CREATE OR REPLACE FUNCTION calculate_peer_points_change_2()
RETURNS TABLE (peer VARCHAR, points_change NUMERIC)
AS $$
BEGIN
    RETURN QUERY
    WITH transfered AS (
    SELECT peer_1 AS peer_name,
        SUM(points_amount) AS points_change
    FROM get_transferred_points_summary()
    GROUP BY peer_1
    UNION ALL
    SELECT peer_2 AS peer_name,
        -SUM(points_amount) AS points_change
    FROM get_transferred_points_summary()
    GROUP BY peer_2
    )
    SELECT peer_name,
        SUM(t.points_change)
    FROM transfered AS t
    GROUP BY peer_name
    ORDER BY  SUM(t.points_change) DESC, peer_name;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM calculate_peer_points_change_2();



-- =========================06=============================

CREATE OR REPLACE FUNCTION fnc_most_frequently_task()
RETURNS TABLE("Day" DATE, "Task" VARCHAR) AS $$
BEGIN
	RETURN QUERY
	WITH TaskCounts AS (
		SELECT "Date", "Checks"."Task", COUNT(*),
			DENSE_RANK() OVER (PARTITION BY "Date" ORDER BY COUNT(*) DESC) AS rn
		FROM "Checks"
		GROUP BY "Date",  "Checks"."Task"
	)
	SELECT "Date" AS "Day", TaskCounts."Task"
	FROM TaskCounts
	WHERE rn = 1
	ORDER BY 1, 2;
END;
$$ LANGUAGE plpgsql;

-- =========================07=============================
CREATE OR REPLACE FUNCTION fnc_block_done(given_block VARCHAR)
RETURNS TABLE("Peer" VARCHAR, "Day" DATE) AS $$
BEGIN
	RETURN QUERY
 	WITH block_task AS
   		(SELECT "Title"
    	FROM "Tasks"
    	WHERE "Title" LIKE given_block || '%'
   		)
	SELECT "Checks"."Peer", MAX("Checks"."Date") AS "Day"
	FROM "Checks"
	JOIN block_task ON block_task."Title" = "Checks"."Task"
	JOIN "XP" ON "Checks"."ID" = "XP"."Check"
	GROUP BY "Checks"."Peer"
	HAVING COUNT(DISTINCT block_task."Title") = (SELECT COUNT(DISTINCT "Title") FROM block_task);
END;
$$ LANGUAGE plpgsql;

--Для проверки
--SELECT * FROM fnc_block_done('SQL')

-- =========================08=============================


DROP FUNCTION IF EXISTS get_recommended_checkers();
CREATE OR REPLACE FUNCTION get_recommended_checkers()
RETURNS TABLE(peer VARCHAR, "RecommendedPeer" VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    WITH friends_recommend AS (
    SELECT p."Nickname",
        r."RecommendedPeer",
        COUNT(r."RecommendedPeer") AS quantity
    FROM "Peers" AS p
    LEFT JOIN "Friends" AS f
        ON f."Peer1" = p."Nickname"
    INNER JOIN "Recommendations" AS r
        ON f."Peer2" = r."Peer"
        AND p."Nickname" != r."RecommendedPeer"
    GROUP BY p."Nickname", r."RecommendedPeer"
    ),
    ranked_recomendations AS (
    SELECT fr."Nickname",
        fr."RecommendedPeer",
        fr.quantity,
        DENSE_RANK() OVER(PARTITION BY
            fr."Nickname"
            ORDER BY fr.quantity DESC) AS rank
    FROM friends_recommend AS fr
    )
    SELECT rr."Nickname",
        rr."RecommendedPeer"
    FROM ranked_recomendations AS rr
    WHERE rank = 1;
END;
$$ LANGUAGE plpgsql;


-- =========================09=============================

CREATE OR REPLACE FUNCTION fnc_start_two_blocks(block_1 VARCHAR, block_2 VARCHAR)
RETURNS TABLE("StartedBlock1" NUMERIC(5, 3), "StartedBlock2" NUMERIC(5, 3),
             "StartedBothBlocks" NUMERIC(5, 3), "DidntStartAnyBlock" NUMERIC(5, 3)) AS $$
BEGIN
    RETURN QUERY
    WITH block_task AS
        (SELECT "Title"
         FROM "Tasks"
         WHERE "Title" LIKE block_1 || '%' OR "Title" LIKE block_2 || '%'
        ),
    start_block_1 AS (
        SELECT "Checks"."Peer"
        FROM "Checks"
        JOIN block_task ON block_task."Title" = "Checks"."Task"
        WHERE "Title" LIKE block_1 || '%'
        GROUP BY "Checks"."Peer"
    ),
    start_block_2 AS (
        SELECT "Checks"."Peer"
        FROM "Checks"
        JOIN block_task ON block_task."Title" = "Checks"."Task"
        WHERE "Title" LIKE block_2 || '%'
        GROUP BY "Checks"."Peer"
    ),
    start_both_block AS (
        SELECT "Peer"
        FROM start_block_1
        INTERSECT
        SELECT "Peer"
        FROM start_block_2
    ),
    all_amount AS (
         SELECT COUNT(*) AS amount
         FROM "Peers"
    )
    SELECT
        ((SELECT COUNT(*) FROM start_block_1)::NUMERIC / (SELECT amount FROM all_amount) * 100)::NUMERIC(5, 3) AS "StartedBlock1",
        ((SELECT COUNT(*) FROM start_block_2)::NUMERIC / (SELECT amount FROM all_amount) * 100)::NUMERIC(5, 3) AS "StartedBlock2",
        ((SELECT COUNT(*) FROM start_both_block)::NUMERIC / (SELECT amount FROM all_amount) * 100)::NUMERIC(5, 3) AS "StartedBothBlocks",
        (((SELECT amount FROM all_amount) - (SELECT COUNT(*) FROM start_block_1) - (SELECT COUNT(*) FROM start_block_2) - (SELECT COUNT(*) FROM start_both_block))::NUMERIC / (SELECT amount FROM all_amount) * 100)::NUMERIC(5, 3) AS "DidntStartAnyBlock";
END;
$$ LANGUAGE plpgsql;

-- select * from fnc_start_two_blocks( 'CPP', 'SQL')


-- =========================10=============================

CREATE OR REPLACE FUNCTION fnc_workaholics_on_birthday()
RETURNS TABLE("SuccessfulChecks" NUMERIC(5, 3), "UnsuccessfulChecks" NUMERIC(5, 3)) AS $$
BEGIN
	RETURN QUERY
	WITH lucky AS (
		SELECT COUNT(*) AS amount
		FROM "Peers"
		JOIN  "Checks" ON "Peers"."Nickname" = "Checks"."Peer"
		JOIN  "XP" ON "Checks"."ID" = "XP"."Check"
		WHERE (EXTRACT(DAY FROM "Peers"."Birthday") = EXTRACT(DAY FROM "Checks"."Date"))
		 	AND (EXTRACT(MONTH FROM "Peers"."Birthday") = EXTRACT(MONTH FROM "Checks"."Date"))
	),
	lol AS (
		SELECT COUNT(*) AS amount
		FROM "Peers"
		LEFT JOIN  "Checks" ON "Peers"."Nickname" = "Checks"."Peer"
		LEFT  JOIN  "XP" ON "Checks"."ID" = "XP"."Check"
		WHERE (EXTRACT(DAY FROM "Peers"."Birthday") = EXTRACT(DAY FROM "Checks"."Date"))
		 	AND (EXTRACT(MONTH FROM "Peers"."Birthday") = EXTRACT(MONTH FROM "Checks"."Date"))
			AND "XP"."XPAmount" IS NULL
	),
	all_amount AS (
		 SELECT COUNT(*) AS amount
		 FROM "Peers"
	)
	SELECT (lucky.amount::NUMERIC / all_amount.amount * 100)::NUMERIC(5, 3) AS "SuccessfulChecks",
		(lol.amount::NUMERIC / all_amount.amount * 100)::NUMERIC(5, 3) AS "UnsuccessfulChecks"
	FROM lucky, lol, all_amount;
END;
$$ LANGUAGE plpgsql;

-- =========================11=============================

 CREATE OR REPLACE FUNCTION fnc_1_2_check_3_not(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR)
RETURNS TABLE("Peer" VARCHAR) AS $$
BEGIN
	RETURN QUERY
	SELECT "Nickname"
	FROM "Peers"
	WHERE "Nickname" IN (SELECT ct."Peer"
						FROM fnc_completed_tasks() AS ct
						WHERE "Task" = task1)
	AND "Nickname" IN (SELECT ct."Peer"
						FROM fnc_completed_tasks() AS ct
						WHERE "Task" = task2)
	AND "Nickname" IN (SELECT ct."Peer"
						FROM fnc_completed_tasks() AS ct
						WHERE "Task" != task3);
END;
$$ LANGUAGE plpgsql;

-- =========================12=============================

CREATE OR REPLACE FUNCTION fnc_recursive()
RETURNS TABLE("TaskTitle" VARCHAR, "PrecedingTasks" INTEGER) AS $$
BEGIN
	RETURN QUERY
	WITH RECURSIVE TaskHierarchy AS (
    	SELECT "Title" AS "TaskTitle", "ParentTask" AS ParentTitle, 0 AS "PrecedingTasks"
   		 FROM "Tasks"
   		 WHERE "ParentTask" = 'None'

    	UNION ALL

    	SELECT t."Title", t."ParentTask", th."PrecedingTasks" + 1
    	FROM "Tasks" t
    	INNER JOIN TaskHierarchy th ON t."ParentTask" = th."TaskTitle"
	)
	SELECT TaskHierarchy."TaskTitle", TaskHierarchy."PrecedingTasks"
	FROM TaskHierarchy;

END;
$$ LANGUAGE plpgsql;


-- =========================13=============================

CREATE OR REPLACE FUNCTION fnc_lucky_day(N INTEGER)
RETURNS TABLE("LuckyDay" DATE) AS $$
BEGIN
	RETURN QUERY
	WITH check_verter_p2p AS
		(SELECT "Check", "State", "Time"
		FROM "P2P"
		UNION
		SELECT "Check", "State", "Time"
		FROM "Verter"),
	all_check AS
		(SELECT *
		FROM "Checks"
		JOIN check_verter_p2p ON check_verter_p2p."Check" = "Checks"."ID"
		ORDER BY "Date", "Time"),
   consecutive_successes AS
    	(SELECT "Date", "State",
        	ROW_NUMBER() OVER (PARTITION BY "Date" ORDER BY "Time") - ROW_NUMBER() OVER (PARTITION BY "Date", "State" ORDER BY "Time") AS "Group"
    	FROM all_check
    ),
    successful_groups AS
    	(SELECT "Date", COUNT(*) AS "ConsecutiveSuccesses"
        FROM consecutive_successes
        WHERE "State" = 'Success'
        GROUP BY "Date", "Group"
        HAVING COUNT(*) >= N
    ),
    successful_days AS
    	(SELECT DISTINCT "Date"
		FROM successful_groups)
	SELECT "Date" AS "LuckyDay"
    FROM successful_days
	ORDER BY "LuckyDay";
END;
$$ LANGUAGE plpgsql;

-- =========================14=============================

 CREATE OR REPLACE FUNCTION fnc_peer_with_max_xp()
RETURNS TABLE("Peer" VARCHAR, "XP" BIGINT) AS $$
BEGIN
	RETURN QUERY
	SELECT ct."Peer", SUM(ct."XP")
    FROM fnc_completed_tasks() AS ct
	GROUP BY ct."Peer"
    ORDER BY 2 DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- =========================15=============================

CREATE OR REPLACE FUNCTION fnc_peers_come_before_time(given_time TIME, n INTEGER)
RETURNS TABLE("Peer" VARCHAR) AS $$
BEGIN
	RETURN QUERY
	SELECT tt."Peer"
	FROM "TimeTracking" AS tt
	WHERE tt."State" = 1
		AND tt."Time" < given_time
	GROUP BY tt."Peer"
	HAVING COUNT(tt."Peer")  >= n;
END;
$$ LANGUAGE plpgsql;

-- =========================16=============================

CREATE OR REPLACE FUNCTION fnc_leaving_peers(M INTEGER, N INTEGER)
RETURNS TABLE("Peer" VARCHAR) AS $$
DECLARE
    current_date DATE := CURRENT_DATE;
BEGIN
	RETURN QUERY
	SELECT tt."Peer"
	FROM "TimeTracking" AS tt
	WHERE tt."State" = 2
		AND tt."Date" > current_date - N
	GROUP BY tt."Peer"
	HAVING COUNT(tt."Peer") > M;
END;
$$ LANGUAGE plpgsql;

-- =========================17=============================

CREATE OR REPLACE FUNCTION fnc_early_entry()
RETURNS TABLE("Month" VARCHAR, "EarlyEntries" NUMERIC(5, 3)) AS $$
BEGIN
	RETURN QUERY
	WITH entrance_counts AS (
		SELECT
			TO_CHAR("Birthday", 'MM')::INTEGER  AS "MonthNumber",
			TO_CHAR("Birthday", 'Month')::VARCHAR AS "Month",
			COUNT(*) AS "TotalEntrances",
			SUM(CASE WHEN EXTRACT(HOUR FROM "Time") < 12 THEN 1 ELSE 0 END) AS "Early"
		FROM "Peers"
		JOIN "TimeTracking" ON "Peers"."Nickname" = "TimeTracking"."Peer"
		WHERE "State" = 1
		GROUP BY "Month", "MonthNumber"
		ORDER BY "MonthNumber"
	)
	SELECT entrance_counts."Month", ROUND(("Early"::NUMERIC / "TotalEntrances"::NUMERIC) * 100, 3) AS "EarlyEntries"
	FROM
		entrance_counts;
END;
$$ LANGUAGE plpgsql;