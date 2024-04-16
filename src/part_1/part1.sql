SET datestyle = DMY;
CREATE
OR REPLACE PROCEDURE "LoadTableFromCSV"(
    table_ VARCHAR,
    path_ VARCHAR,
    delimiter_ CHAR(1)
) LANGUAGE plpgsql AS $$
BEGIN EXECUTE 'COPY "' || table_ || '" FROM ' || quote_literal(path_) || ' WITH (FORMAT csv, DELIMITER ' || quote_literal(delimiter_) || ', HEADER)';
END;
$$;
CREATE
OR REPLACE PROCEDURE "SaveTableToCSV"(
    table_ VARCHAR,
    path_ VARCHAR,
    delimiter_ CHAR(1)
) LANGUAGE plpgsql AS $$
BEGIN EXECUTE 'COPY "' || table_ || '" TO ' || quote_literal(path_) || ' WITH (FORMAT csv, DELIMITER ' || quote_literal(delimiter_) || ', HEADER)';
END;
$$;
-- Создание таблицы Peers
CREATE TABLE IF NOT EXISTS  "Peers"(
    "Nickname" VARCHAR PRIMARY KEY,
    "Birthday" DATE NOT NULL
);
-- Создание таблицы Tasks
CREATE TABLE IF NOT EXISTS "Tasks" (
    "Title" VARCHAR PRIMARY KEY,
    "ParentTask" VARCHAR,
    "MaxXP" INTEGER NOT NULL
);
-- Создание таблицы Checks
CREATE TABLE IF NOT EXISTS "Checks" (
    "ID" SERIAL PRIMARY KEY,
    "Peer" VARCHAR NOT NULL REFERENCES "Peers"("Nickname"),
    "Task" VARCHAR NOT NULL REFERENCES "Tasks"("Title"),
    "Date" DATE NOT NULL
);
-- Создание типа перечисления для статуса проверки
CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');
-- Создание таблицы P2P
CREATE TABLE IF NOT EXISTS "P2P" (
    "ID" BIGINT PRIMARY KEY,
    "Check" BIGINT NOT NULL REFERENCES "Checks"("ID"),
    "CheckingPeer" VARCHAR NOT NULL REFERENCES "Peers"("Nickname"),
    "State" check_status DEFAULT 'Start',
    "Time" TIME NOT NULL
);
-- Создание таблицы Verter
CREATE TABLE IF NOT EXISTS "Verter" (
    "ID" BIGINT PRIMARY KEY,
    "Check" BIGINT NOT NULL REFERENCES "Checks"("ID"),
    "State" check_status DEFAULT 'Start',
    "Time" TIME NOT NULL
);
-- Создание таблицы TransferredPoints
CREATE TABLE IF NOT EXISTS "TransferredPoints" (
    "ID" BIGINT PRIMARY KEY,
    "CheckingPeer" VARCHAR NOT NULL REFERENCES "Peers"("Nickname"),
    "CheckedPeer" VARCHAR NOT NULL REFERENCES "Peers"("Nickname") CHECK ("CheckedPeer" != "CheckingPeer"),
    "PointsAmount" INTEGER CHECK ("PointsAmount" > 0)
);
-- Создание таблицы Friends
CREATE TABLE IF NOT EXISTS "Friends" (
    "ID" BIGINT PRIMARY KEY,
    "Peer1" VARCHAR NOT NULL REFERENCES "Peers"("Nickname"),
    "Peer2" VARCHAR NOT NULL REFERENCES "Peers"("Nickname")
);
-- Создание таблицы Recommendations
CREATE TABLE IF NOT EXISTS "Recommendations" (
    "ID" BIGINT PRIMARY KEY,
    "Peer" VARCHAR NOT NULL REFERENCES "Peers"("Nickname"),
    "RecommendedPeer" VARCHAR REFERENCES "Peers"("Nickname") CHECK ("RecommendedPeer" != "Peer")
);
-- Создание таблицы XP
CREATE TABLE IF NOT EXISTS "XP" (
    "ID" BIGINT PRIMARY KEY,
    "Check" INTEGER NOT NULL REFERENCES "Checks"("ID"),
    "XPAmount" INTEGER NOT NULL
);
-- Создание таблицы TimeTracking
CREATE TABLE IF NOT EXISTS "TimeTracking" (
    "ID" BIGINT PRIMARY KEY,
    "Peer" VARCHAR NOT NULL REFERENCES "Peers"("Nickname"),
    "Date" DATE NOT NULL,
    "Time" TIME NOT NULL,
    "State" INTEGER NOT NULL CHECK (
        "State" = 1
        OR "State" = 2
    )
);
