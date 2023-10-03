CREATE TABLE employees
(employee_id INT PRIMARY KEY,
first_name TEXT,
last_name TEXT,
title TEXT,
birth_date DATE,
notes TEXT);

CREATE TABLE customers(
    customer_id TEXT PRIMARY KEY,
    company_name TEXT,
    contact_name TEXT,
   );

CREATE TABLE orders(
    order_id INT PRIMARY KEY,
    customer_id TEXT,
    employee_id	 INT,
    order_date INT,
    ship_city TEXT,);


