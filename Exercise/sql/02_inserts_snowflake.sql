-- sql/02_inserts_snowflake.sql
USE DATABASE ANALYTICS;
USE SCHEMA PUBLIC;

-- Customers
INSERT INTO DIM_CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE, COUNTRY) VALUES
('Alice','Johnson','alice.johnson@example.com','New York','NY','USA'),
('Bob','Smith','bob.smith@example.com','Chicago','IL','USA'),
('Charlie','Lee','charlie.lee@example.com','San Francisco','CA','USA'),
('Diana','Martinez','diana.martinez@example.com','Miami','FL','USA'),
('Ethan','Wong','ethan.wong@example.com','Seattle','WA','USA');

-- Products
INSERT INTO DIM_PRODUCTS (PRODUCT_NAME,CATEGORY,BRAND,UNIT_PRICE) VALUES
('Wireless Mouse','Electronics','LogiTech',29.99),
('Mechanical Keyboard','Electronics','KeyChron',89.99),
('Running Shoes','Sportswear','Nike',120.00),
('Smartphone','Electronics','Apple',999.00),
('Coffee Maker','Home Appliances','Breville',199.00);

-- Dates
INSERT INTO DIM_DATE (CALENDAR_DATE,YEAR,MONTH,DAY,DAY_OF_WEEK,IS_WEEKEND) VALUES
('2025-08-15',2025,8,15,'Friday',FALSE),
('2025-08-16',2025,8,16,'Saturday',TRUE),
('2025-08-17',2025,8,17,'Sunday',TRUE),
('2025-08-18',2025,8,18,'Monday',FALSE),
('2025-08-19',2025,8,19,'Tuesday',FALSE);

-- Orders
INSERT INTO FACT_ORDERS (CUSTOMER_ID,PRODUCT_ID,DATE_ID,QUANTITY,UNIT_PRICE,TOTAL_AMOUNT,ORDER_STATUS) VALUES
(1,1,1,2,29.99,59.98,'COMPLETED'),
(2,2,2,1,89.99,89.99,'COMPLETED'),
(3,3,3,1,120.00,120.00,'COMPLETED'),
(4,4,4,1,999.00,999.00,'COMPLETED'),
(5,5,5,2,199.00,398.00,'COMPLETED'),
(1,3,2,1,120.00,120.00,'COMPLETED'),
(2,1,3,3,29.99,89.97,'COMPLETED'),
(3,5,4,1,199.00,199.00,'COMPLETED');
