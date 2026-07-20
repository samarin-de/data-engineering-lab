--Write your MySQL query statement below
SELECT MAX(salary) AS SecondHighestSalary
FROM Employee
WHERE salary < (SELECT MAX(salary) FROM Employee)
--176
--
WITH ranked AS (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM Employee
)
SELECT MAX(salary) AS SecondHighestSalary
FROM ranked
WHERE rnk = 2;

--178
WITH ranked AS (
    SELECT score,
    DENSE_RANK() OVER (ORDER BY score DESC) AS rank
    FROM Scores
)
SELECT score, rank
FROM ranked
ORDER BY rank;

---180
WITH lagged AS (
    SELECT
    id,
    num,
    LAG(num, 1) OVER (ORDER BY id) AS prev1,
    LAG(num, 2) OVER (ORDER BY id) AS prev2
    FROM Logs
) 
SELECT DISTINCT num AS ConsecutiveNums
FROM lagged
WHERE num=prev1 AND num=prev2;