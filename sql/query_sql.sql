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