-- ============================================
-- Название: Цена монеты + LAG (предыдущая цена)
-- Задача: Соединить факты с измерениями и вычислить разницу с прошлым днем
-- Техника: Оконные функции, Hash Join
-- Дата: 2026-07-15
-- ============================================

SELECT
    c.full_name AS coin_name,
    d.full_date,
    f.price_usd,
    LAG(f.price_usd) OVER (PARTITION BY f.coin_id ORDER BY d.full_date) AS prev_price,
    f.price_usd - LAG(f.price_usd) OVER (PARTITION BY f.coin_id ORDER BY d.full_date) AS diff
FROM fact_crypto_rates f
JOIN dim_coins c ON f.coin_id = c.coin_id
JOIN dim_calendar d ON f.date_id = d.date_id
ORDER BY c.full_name, d.full_date;


-- ============================================
-- ПЛАН ВЫПОЛНЕНИЯ (EXPLAIN ANALYZE)
-- Результат: Execution Time: 0.089 ms (супер-быстро, т.к. данных мало)
-- Узлы: Hash Join + WindowAgg
-- ============================================
/*
 Sort  (cost=160.72..162.74 rows=810 width=310) (actual time=0.053..0.055 rows=4 loops=1)
   Sort Key: c.full_name, d.full_date
   Sort Method: quicksort  Memory: 25kB
   ->  WindowAgg  (cost=103.36..121.59 rows=810 width=310) (actual time=0.042..0.048 rows=4 loops=1)
         ... (и так далее, можно вставить весь вывод)
 Planning Time: 0.144 ms
 Execution Time: 0.089 ms
*/

--===================================================
--LEFT JOIN
SELECT
    c.full_name AS coin_name,
    COUNT(f.price_usd) AS recods_count,
    MIN(d.full_date) AS first_date,
    MAX(d.full_date) AS last_date
FROM dim_coins c 
LEFT JOIN fact_crypto_rates f ON c.coin_id = f.coin_id
LEFT JOIN dim_calendar d ON f.date_id = d.date_id
GROUP BY c.full_name
ORDER BY c.full_name;

--================================================
--EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT
    c.full_name AS coin_name,
    COUNT(f.price_usd) AS recods_count,
    MIN(d.full_date) AS first_date,
    MAX(d.full_date) AS last_date
FROM dim_coins c
LEFT JOIN fact_crypto_rates f ON c.coin_id = f.coin_id
LEFT JOIN dim_calendar d ON f.date_id = d.date_id
GROUP BY c.full_name
ORDER BY c.full_name;
--=================================================
--INDEX
EXPLAIN ANALYZE
SELECT * FROM fact_crypto_rates
WHERE coin_id = 1; -- Bitcoin coin_id = 1

--создать индекс coin_id
CREATE INDEX idx_fact_coin_id ON fact_crypto_rates(coin_id);
--На маленькой таблице индекс не используется автоматически, 
--но при большом объёме данных ускоряет запрос
SET enable_seqscan = off; --on
--===================
DROP INDEX idx_fact_coin_id;
--Составной индекс
CREATE INDEX idx_fact_coin_date ON fact_crypto_rates(coin_id, date_id);
--На малых данных индекс не используется оптимизатором PostgreSQL 
EXPLAIN ANALYZE
SELECT * FROM fact_crypto_rates
WHERE coin_id = 1
ORDER BY date_id;