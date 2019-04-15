CREATE TABLE TOP_EXPERIENCED AS(
    SELECT employeeId, name, surname, country, experience, department FROM
    EMPLOYEES AS e JOIN EMPLOYEE_DETAILS AS ed ON e.id = ed.employeeId
    GROUP BY country, department
    ORDER BY experience DESC
    LIMIT ${hivevar:topExperience}
) STORED AS ORC;

CREATE TABLE TOP_EARNERS AS(
    SELECT employeeId, name, surname, country, salary, department FROM
    EMPLOYEES AS e JOIN EMPLOYEE_DETAILS AS ed ON e.id = ed.employeeId
    JOIN SALARY_DATA AS s ON e.id = s.employeeId
    GROUP BY country, department
    ORDER BY salary DESC
    LIMIT ${hivevar:topEarners}
) STORED AS ORC;

CREATE TABLE TOP_BOTH AS(
    SELECT employeeId, name, surname, country, salary, department, experience FROM
    TOP_EXPERIENCED AS t1 JOIN TOP_EARNERS AS t2 ON t1.employeeId = t2.employeeId
    LIMIT ${hivevar:topBoth}
) STORED AS ORC;