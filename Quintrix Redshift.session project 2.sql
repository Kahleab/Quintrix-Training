CREATE SCHEMA lkp_data;
USE lkp_data;



CREATE TABLE lkp_state_details(
    state_cd VARCHAR
    population_cnt int
    potential_customer_cnt int
)

INSERT INTO lkp_state_details
    (state_cd,population_cnt,potential_customer_cnt)
VALUES
    ('NY',200,100),
    ('CA',500,200),
    ('TX',400,300),
    ('NV',100,90),
    ('NJ',200,70),
    ('PA',300,200);


-- Question 1
SELECT *FROM cards_ingest.tran_fact
FULL OUTER JOIN  lkp_state_details
ON cards_ingest.tran_fact.stat_cd = lkp_state_details.state_cd
WHERE tran_id,cust_id, stat_cd, tran_ammt,tran_date IS NOT NULL


--Question 2
SELECT potetial_customer_cnt
potential_customer_cnt * 5  AS total_cost
FROM lkp_state_details
RANK()OVER(ORDER BY total_cost)AS RANK_NO

--Question 3
SELECT *FROM cards_ingest.tran_fact
FULL OUTER JOIN  lkp_state_details
ON cards_ingest.tran_fact.stat_cd = lkp_state_details.state_cd
WHERE tran_id,cust_id, stat_cd, tran_ammt,tran_date IS NOT NULL
WHERE tran_date >= '2022-02-01'

--Question 4
SELECT potetial_customer_cnt
potential_customer_cnt * 5  AS total_cost
CASE
    WHEN state_cd AND cust_id = cust_109 THEN state_cd = 'TX'
    ELSE state_cd = 'CA'
FROM lkp_state_details
RANK()OVER(ORDER BY total_cost)AS RANK_NO


--Question 5
SELECT state_cd
COUNT(potential_customer_cnt)
OVER([PARTITION BY state_cd] [ORDER BY population_cnt])
AS total_cust
FROM lkp_state_details
