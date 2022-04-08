1.
SELECT
A.emp_name,
A.job_name,
A.dep_id,
A.salary AS emp_salary,
B.salary AS chief_salary
FROM employees AS A
JOIN employees AS BS
ON B.emp_id = A.manager_id
WHERE A.salary > B.salary;


2. 
SELECT A.emp_name, A.job_name, A.dep_id, A.salary
FROM employees AS A
WHERE A.salary = (SELECT MIN(B.salary) FROM employees AS B WHERE  B.dep_id = A.dep_id);


3.
SELECT dep_id
FROM employees
GROUP BY dep_id
HAVING COUNT(*) > 3;


4.
SELECT A.emp_name, A.job_name, C.dep_name
FROM employees AS A
LEFT JOIN employees AS B ON (B.emp_id = A.manager_id AND B.dep_id = A.dep_id)
JOIN department AS C ON A.dep_id = C.dep_id
WHERE B.emp_id IS NULL;


5.

SELECT A.*, DENSE_RANK() OVER(PARTITION BY dep_id ORDER BY exp_days DESC) AS rank
FROM
(SELECT emp_name, job_name,  dep_id, (current_date - hire_date) as exp_days
FROM employees) as A;


6.
SELECT COUNT(*) as total, salary
FROM employees
GROUP BY salary
ORDER BY total DESC