SELECT id, name, role
FROM employees
UNION ALL
SELECT id, name, role
FROM contractors;