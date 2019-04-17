-- Create a table, storing max value of salary and experience per-country and per-department
CREATE TABLE max_values AS SELECT MAX(sd.salary) AS max_salary, MAX(ed.experience) AS max_experience, country, department FROM EMPLOYMENT_DETAILS AS ed JOIN SALARY_DATA AS sd ON ed.employeeId = sd.employeeId GROUP BY country, department;

-- Connect user data of all types with the pre-aggregated max salaries and experience to find employees that have both at once, in the respective departments and countries
SELECT e.name, e.surname, sd.salary, ed.experience, mv.country, mv.department FROM employees AS e JOIN employment_details AS ed ON e.id = ed.employeeId JOIN salary_data AS sd on e.id = sd.employeeId JOIN max_values AS mv ON sd.salary = mv.max_salary AND ed.experience = mv.max_experience;