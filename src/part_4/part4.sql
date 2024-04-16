-- =============================01=================================

CREATE OR REPLACE PROCEDURE drop_tables_with_table_name()
AS $$
DECLARE
    name_of_table RECORD;
BEGIN
    FOR name_of_table IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = current_schema() AND table_name LIKE 'TableName%'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || name_of_table.table_name || ' CASCADE';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- =============================02=================================

CREATE OR REPLACE PROCEDURE get_scalar_functions(OUT function_count INTEGER)
AS $$
DECLARE
    function_record RECORD;
    function_info VARCHAR := ''; -- Переменная для хранения информации о функциях
BEGIN
    function_count := 0;

    FOR function_record IN 
        SELECT r.routine_name || '(' || STRING_AGG(p.parameter_name || ' ' || p.data_type, ', ') || ')' AS function_info
        -- information_schema.routines - это представление, которое содержит информацию о функциях и процедурах в базе данных
        FROM information_schema.routines r
        --information_schema.parameters  представление , которое содержит информацию о параметрах функций и процедур в базе данных
        JOIN information_schema.parameters p ON r.specific_name = p.specific_name 
         --  системный каталог pg_proc хранит информацию о всех функциях и процедурах в базе данных
        JOIN pg_proc proc ON proc.proname = r.routine_name
        WHERE r.specific_schema = current_schema()
            AND r.routine_type = 'FUNCTION'
            AND p.parameter_mode = 'IN'
            AND proc.proretset = false -- Фильтр для скалярных функций. proretset указывает, возвращает ли функция набор строк
        GROUP BY r.routine_name
    LOOP
        function_info := function_info || '   ' || function_record.function_info;
        function_count := function_count + 1;
    END LOOP;

    RAISE NOTICE 'Functions: %', function_info;
END;
$$ LANGUAGE plpgsql;

-- Пример скалярной функции для проверки
-- CREATE OR REPLACE FUNCTION calcy(radius NUMERIC)
-- RETURNS NUMERIC AS $$
-- DECLARE
--     area NUMERIC;
-- BEGIN
--     area := 3.14 * radius * radius;
--     RETURN area;
-- END;
-- $$ LANGUAGE plpgsql;

-- Вызов процедуру	
-- DO $$
-- DECLARE
--     function_count INTEGER;
-- BEGIN
--     CALL get_scalar_functions(function_count);
--     RAISE NOTICE 'Total scalar functions: %', function_count;
-- END $$;
-- =============================03=================================

-- Таблица information_schema.triggers содержит метаданные о триггерах базы данных. Вот некоторые из основных столбцов этой таблицы:

CREATE OR REPLACE PROCEDURE destroy_trigger(OUT result INTEGER)
AS $$
DECLARE
    trigger_record RECORD;
    cnt_destroy_trigget INTEGER := 0;
BEGIN

    FOR trigger_record IN 
        SELECT trigger_name, event_object_table 
        FROM information_schema.triggers 
    LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS ' || trigger_record.trigger_name  || ' ON "' || trigger_record.event_object_table || '" CASCADE';
        cnt_destroy_trigget := cnt_destroy_trigget + 1;
    END LOOP;
    result := cnt_destroy_trigget;
END;
$$ LANGUAGE plpgsql;

-- DO $$
-- DECLARE
--     trigger_count INTEGER;
-- BEGIN
--     CALL destroy_trigger(trigger_count);
--     RAISE NOTICE 'Total trigger destroy: %', trigger_count;
-- END $$;

-- =============================04=================================

CREATE OR REPLACE FUNCTION get_definition_object(str VARCHAR)
RETURNS TABLE(object_name VARCHAR, object_description TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT r.routine_name::VARCHAR, r.routine_definition::TEXT
    FROM information_schema.routines r
    LEFT JOIN information_schema.parameters p ON r.specific_name = p.specific_name
    JOIN pg_proc proc ON proc.proname = r.routine_name
    WHERE (r.routine_type = 'PROCEDURE' OR (
            r.routine_type = 'FUNCTION'
            AND p.parameter_mode = 'IN'
            AND proc.proretset = false ))
        AND r.routine_definition LIKE '%' || str || '%';
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_definition_object('DROP TRIGGER');