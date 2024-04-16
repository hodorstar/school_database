CREATE OR REPLACE PROCEDURE "AddP2PCheck"(
    checking_peer_ VARCHAR,
    checked_peer_ VARCHAR,
    task_title_ VARCHAR,
    p2p_status_ check_status,
    p2p_time_ TIME
) LANGUAGE plpgsql AS $$
DECLARE
    check_id_ BIGINT;
BEGIN
    -- Добавление записи в таблицу Checks, если статус "Start"
    IF p2p_status_ = 'Start' THEN
        INSERT INTO "Checks" ("ID", "Peer", "Task", "Date") VALUES ((SELECT COALESCE(max("ID"), 0) + 1 FROM "Checks"),
        checked_peer_,
        task_title_,
        CURRENT_DATE)
        RETURNING "ID" INTO check_id_;
    ELSE
        --Поиск незавершенной проверки для данного проверяющего и проверяемого
        SELECT "ID" INTO check_id_
        FROM "Checks"
        WHERE "Peer" = checked_peer_
        AND "Task" = task_title_
        AND "Date" = CURRENT_DATE
        AND NOT EXISTS (
            SELECT 1
            FROM "P2P"
            WHERE "Check" = "Checks"."ID"
            AND "State" = 'Start'
        );

    END IF;
    -- Добавление записи в таблицу P2P

    INSERT INTO "P2P" ("ID", "Check", "CheckingPeer", "State", "Time") VALUES
        ((SELECT COALESCE(max("ID"), 0) + 1 FROM "P2P"),
        (SELECT "ID" FROM "Checks" WHERE "ID"=check_id_),
        (SELECT "Nickname" FROM "Peers" WHERE "Nickname"=checking_peer_),
        p2p_status_,
        p2p_time_
    );
END;
$$;


-- CALL "AddP2PCheck"('hwcnyoyyqt', 'knpumfeeqx', 'DO1', 'Start', '17:50');

-- select * from "Checks" as c
-- order by "ID" desc
-- LIMIT 1;

-- Select * FROM "P2P"
-- order by "ID" desc
-- LIMIT 1;

---------------------------------

CREATE OR REPLACE PROCEDURE "AddVerterCheck"(
    checked_peer_ VARCHAR,
    task_title_ VARCHAR,
    verter_status_ check_status,
    verter_time_ TIME
) LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO "Verter" ("ID", "Check", "State", "Time") VALUES
    ((SELECT COALESCE(max("ID"), 0) + 1 FROM "Verter"),
    (SELECT c."ID" FROM "Checks" AS c
        JOIN "P2P" AS p ON p."Check" = c."ID"
        WHERE c."Task" = task_title_
        AND p."State" = 'Success'
        ORDER BY "Date", "Time" DESC
        LIMIT 1),
    verter_status_,
    verter_time_
    );
    END;
$$;


-- select * from "Verter" as c
-- order by "ID" desc
-- LIMIT 1;

-- CALL "AddVerterCheck"('hwcnyoyyqt', 'DO1', 'Start', '17:50');

-- select * from "Verter" as c
-- order by "ID" desc
-- LIMIT 1;

----------------------------------------

CREATE OR REPLACE FUNCTION updateTransferredPoints()
RETURNS TRIGGER AS $$
DECLARE
    count_review_ INTEGER;
    checked_peer_ VARCHAR;
BEGIN
	SELECT "Peer" INTO checked_peer_
    FROM "P2P" as pp
    JOIN "Checks" AS c ON c."ID" = pp."Check"
    ORDER BY pp."ID" DESC
    LIMIT 1;

    SELECT COUNT(c."ID") INTO count_review_
    FROM "Checks" AS c
    JOIN "P2P" AS p ON p."Check" = c."ID"
    WHERE c."Peer" = checked_peer_
    AND p."CheckingPeer" = NEW."CheckingPeer";

    IF NEW."State" = 'Start' THEN
        -- Обновление записи в таблице TransferredPoints
        IF count_review_ > 0 THEN

            UPDATE "TransferredPoints"
            SET "PointsAmount" = "PointsAmount" + 1
            WHERE "CheckingPeer" = NEW."CheckingPeer"
            AND "CheckedPeer" = checked_peer_;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS P2P_Status_Start_Trigger ON "P2P";
CREATE TRIGGER P2P_Status_Start_Trigger
AFTER INSERT ON "P2P"
FOR EACH ROW
EXECUTE FUNCTION updateTransferredPoints();

-- INSERT INTO "P2P" VALUES
--     ((SELECT COALESCE(max("ID"), 0) + 1 FROM "P2P"),
--     (SELECT "ID" FROM "Checks" WHERE "ID"=13930),
--     (SELECT "Nickname" FROM "Peers" WHERE "Nickname"='cpwfiewvim'),
--     'Start',
--     CURRENT_TIME);

-- SELECT * FROM "TransferredPoints"
-- WHERE "CheckingPeer" = 'cpwfiewvim'
-- AND "CheckedPeer" = 'vrbyeonaxg';


----------------------------------


CREATE OR REPLACE FUNCTION CheckXP()
RETURNS TRIGGER AS $$
DECLARE
    max_xp INTEGER;
BEGIN
    SELECT "MaxXP" INTO max_xp
    FROM "Checks" as c
    JOIN "Tasks" as t on t."Title" = c."Task"
    WHERE "Title" = "Task"
    AND NEW."Check" = c."ID";

    IF NEW."XPAmount" <= max_xp THEN
        IF EXISTS (
            SELECT 1
            FROM "Checks" AS c
            JOIN "P2P" AS p ON p."Check" = c."ID"
            WHERE c."ID" = NEW."Check"
            AND p."State" = 'Success'
        ) THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'The check for the XP record is not successful.';
        END IF;
    ELSE
        RAISE EXCEPTION 'The XP amount exceeds the maximum allowed for this task.';
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS XP_Trigger ON "XP";
CREATE TRIGGER XP_Trigger
BEFORE INSERT ON "XP"
FOR EACH ROW
EXECUTE FUNCTION CheckXP();

-- SELECT * FROM "XP"
-- ORDER BY "ID" DESC
-- LIMIT 1;

-- INSERT INTO "XP" VALUES
--     ((SELECT COALESCE(max("ID"), 0) + 1 FROM "XP"),
--     (SELECT "ID" FROM "Checks" WHERE "ID"=13928),
--     344);


-- INSERT INTO "XP" VALUES
--     ((SELECT COALESCE(max("ID"), 0) + 1 FROM "XP"),
--     (SELECT "ID" FROM "Checks" WHERE "ID"=13930),
--     344);

-- SELECT * FROM "XP"
-- ORDER BY "ID" DESC
-- LIMIT 1;
