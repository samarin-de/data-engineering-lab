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