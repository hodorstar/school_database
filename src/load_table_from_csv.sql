-- Скрип для загрузки данных из csv-файлов
DO $$DECLARE
-- base_path - абсолютный путь до csv-файлов
    base_path TEXT := '';
BEGIN
    CALL "LoadTableFromCSV"('Peers', base_path || 'peers.csv', ';');
    CALL "LoadTableFromCSV"('Tasks', base_path || 'tasks.csv', ';');
    CALL "LoadTableFromCSV"('Checks', base_path || 'checks.csv', ';');
    CALL "LoadTableFromCSV"('P2P', base_path || 'P2P.csv', ';');
    CALL "LoadTableFromCSV"('Verter', base_path || 'verter.csv', ';');
    CALL "LoadTableFromCSV"('TransferredPoints', base_path || 'transferred_points.csv', ';');
    CALL "LoadTableFromCSV"('Friends', base_path || 'friends.csv', ';');
    CALL "LoadTableFromCSV"('Recommendations', base_path || 'recommendations.csv', ';');
    CALL "LoadTableFromCSV"('XP', base_path || 'xp.csv', ';');
    CALL "LoadTableFromCSV"('TimeTracking', base_path || 'time_tracking.csv', ';');
END $$