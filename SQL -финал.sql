CREATE DATABASE fin_proekt;

CREATE TABLE  customer_info (
	Id_client INT,
    Total_amount INT,
    Gender VARCHAR (10),
    Age INT NULL,
    Count_city INT,
    Response_communcation INT,
    Communication_3month INT,
	Tenure INT
    );
SELECT * FROM customer_info;

CREATE TABLE transactions_info (
    date_new DATE NOT NULL,
    Id_check INT NOT NULL,
    ID_client INT NOT NULL,
    Count_products DECIMAL(10, 3),
    Sum_payment DECIMAL(10, 2)
);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions_info.xlsx - TRANSACTIONS (1).csv"
INTO TABLE transactions_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'secure_file_priv';
       
       
SELECT * FROM transactions_info;

# 1. Список клиентов с непрерывной историей за год
WITH monthly_transactions AS (
    SELECT
        ID_client,
        DATE_FORMAT(date_new, '%Y-%m-01') AS transaction_month,
        COUNT(*) AS transaction_count,
        SUM(Sum_payment) AS total_payment
    FROM transactions_info
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
    GROUP BY ID_client, transaction_month
),
continuous_history AS (
    SELECT
        ID_client,
        COUNT(DISTINCT transaction_month) AS months_active
    FROM monthly_transactions
    GROUP BY ID_client
    HAVING COUNT(DISTINCT transaction_month) = 12
)
SELECT
    c.ID_client,
    AVG(mt.total_payment) AS average_check,  
    AVG(mt.total_payment / mt.transaction_count) AS average_monthly_purchase,
    SUM(mt.transaction_count) AS total_transactions,
    c.gender
FROM continuous_history ch
JOIN monthly_transactions mt ON ch.ID_client = mt.ID_client
JOIN customer_info c ON c.ID_client = ch.ID_client
GROUP BY c.ID_client, c.gender; 

# 2. Информация в разрезе месяцев
	# a) Средняя сумма чека в месяц
SELECT
	DATE_FORMAT(date_new, '%Y-%m-01') AS transaction_month,
    AVG(Sum_payment) AS average_check
FROM transactions_info
WHERE date_new>= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY transaction_month;

	# b) Среднее количество операций в месяц
SELECT
    transaction_month, AVG(transaction_count) AS average_transactions
FROM (SELECT DATE_FORMAT(date_new, '%Y-%m-01') AS transaction_month,
        COUNT(*) AS transaction_count
    FROM transactions_info
    WHERE date_new>= '2015-06-01' AND date_new < '2016-06-01'
    GROUP BY transaction_month, ID_client
) AS monthly_summary
GROUP BY transaction_month;

	# c) Среднее количество клиентов, которые совершали операции
SELECT transaction_month,
    COUNT(DISTINCT ID_client) AS active_customers
FROM (SELECT DATE_FORMAT(date_new, '%Y-%m-01') AS transaction_month, ID_client
    FROM transactions_info
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
) AS monthly_customers
GROUP BY transaction_month;

	#  d) Доля от общего количества операций за год и доля в месяц от общей суммы операций
WITH total_operations AS (
    SELECT COUNT(*) AS total_count, SUM(Sum_payment) AS total_payment
    FROM transactions_info
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
)
SELECT month,  
    COUNT(*) AS monthly_operations,
    COUNT(*) * 100.0 / (SELECT total_count FROM total_operations) AS operation_ratio_year,
    SUM(Sum_payment) AS monthly_total_payment,
    SUM(Sum_payment) * 100.0 / (SELECT total_payment FROM total_operations) AS amount_ratio_year
FROM (SELECT DATE_FORMAT(date_new, '%Y-%m-01') AS month, Sum_payment
      FROM transactions_info
      WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
     ) AS monthly_summary
GROUP BY month;

	# e) % соотношение M/F/NA в каждом месяце с их долей затрат
SELECT month, Gender,
    COUNT(*) AS Gender_count,
    SUM(Sum_payment) AS Gender_Sum_payment,
    SUM(Sum_payment) * 100.0 / SUM(SUM(Sum_payment)) OVER(PARTITION BY month) AS Gender_ratio
FROM (SELECT DATE_FORMAT(date_new, '%Y-%m-01') AS month,
        c.Gender, t.Sum_payment
    FROM transactions_info t
    JOIN customer_info c ON t.ID_client = c.ID_client
    WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
) AS monthly_Gender_summary
GROUP BY month, Gender;



	# 3.возрастные группы клиентов с шагом 10 лет и отдельно клиентов, у которых нет данной информации, 
    # с параметрами сумма и количество операций за весь период, и поквартально - средние показатели и %.

SELECT CASE 
        WHEN c.Age IS NULL THEN 'Не указано'
        WHEN c.Age < 10 THEN '0-9'
        WHEN c.Age < 20 THEN '10-19'
        WHEN c.Age < 30 THEN '20-29'
        WHEN c.Age < 40 THEN '30-39'
        WHEN c.Age < 50 THEN '40-49'
        WHEN c.Age < 60 THEN '50-59'
        WHEN c.Age < 70 THEN '60-69'
        WHEN c.Age < 80 THEN '70-79'
        ELSE '80+'
    END AS age_group,
    COUNT(t.Id_check) AS Id_check_count,
    SUM(t.Sum_payment) AS total_Sum_payment
FROM customer_info c
LEFT  JOIN
    transactions_info t ON c.ID_client = t.ID_client
GROUP BY age_group;
    
  #    Средние показатели и % поквартально
SELECT YEAR(t.date_new ) AS year,
    QUARTER(t.date_new ) AS quarter,
    COUNT(t.Id_check) AS Id_check_count,
    SUM(t.Sum_payment) AS total_Sum_payment,
    AVG(t.Sum_payment) AS average_Sum_payment
FROM transactions_info t
JOIN customer_info c ON c.ID_client = t.ID_client
GROUP BY year, quarter
ORDER BY year, quarter;

WITH total_transactions AS (
    SELECT COUNT(Id_check) AS Id_check_count, SUM(Sum_payment) AS total_sum
    FROM transactions_info
)
SELECT YEAR(t.date_new) AS year,
    QUARTER(t.date_new) AS quarter,
    COUNT(t.Id_check) AS Id_check_count,
    SUM(t.Sum_payment) AS total_Sum_payment,
    AVG(t.Sum_payment) AS average_Sum_payment,
    (COUNT(t.Id_check) / (SELECT Id_check_count FROM total_transactions)) * 100 AS percentage_count,
    (SUM(t.Sum_payment) / (SELECT total_sum FROM total_transactions)) * 100 AS percentage_sum
FROM transactions_info t
JOIN customer_info c ON c.ID_client = t.ID_client
GROUP BY year, quarter
ORDER BY year, quarter;