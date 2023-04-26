create database if not exists demo;
use database demo;
create schema snowpark_examples;
use schema demo.snowpark_examples;


CREATE OR REPLACE TABLE sample_product_data (id INT, parent_id INT, category_id INT, name VARCHAR, serial_number VARCHAR, key INT, "3rd" INT, amount NUMBER(12, 2), quantity INT, product_date DATE);
INSERT INTO sample_product_data VALUES
    (1, 0, 5, 'Product 1', 'prod-1', 1, 10, 1.00, 15, TO_DATE('2021.01.01', 'YYYY.MM.DD')),
    (2, 1, 5, 'Product 1A', 'prod-1-A', 1, 20, 2.00, 30, TO_DATE('2021.02.01', 'YYYY.MM.DD')),
    (3, 1, 5, 'Product 1B', 'prod-1-B', 1, 30, 3.00, 45, TO_DATE('2021.03.01', 'YYYY.MM.DD')),
    (4, 0, 10, 'Product 2', 'prod-2', 2, 40, 4.00, 60, TO_DATE('2021.04.01', 'YYYY.MM.DD')),
    (5, 4, 10, 'Product 2A', 'prod-2-A', 2, 50, 5.00, 75, TO_DATE('2021.05.01', 'YYYY.MM.DD')),
    (6, 4, 10, 'Product 2B', 'prod-2-B', 2, 60, 6.00, 90, TO_DATE('2021.06.01', 'YYYY.MM.DD')),
    (7, 0, 20, 'Product 3', 'prod-3', 3, 70, 7.00, 105, TO_DATE('2021.07.01', 'YYYY.MM.DD')),
    (8, 7, 20, 'Product 3A', 'prod-3-A', 3, 80, 7.25, 120, TO_DATE('2021.08.01', 'YYYY.MM.DD')),
    (9, 7, 20, 'Product 3B', 'prod-3-B', 3, 90, 7.50, 135, TO_DATE('2021.09.01', 'YYYY.MM.DD')),
    (10, 0, 50, 'Product 4', 'prod-4', 4, 100, 7.75, 150, TO_DATE('2021.10.01', 'YYYY.MM.DD')),
    (11, 10, 50, 'Product 4A', 'prod-4-A', 4, 100, 8.00, 165, TO_DATE('2021.11.01', 'YYYY.MM.DD')),
    (12, 10, 50, 'Product 4B', 'prod-4-B', 4, 100, 8.50, 180, TO_DATE('2021.12.01', 'YYYY.MM.DD'));



CREATE OR REPLACE TABLE sample_a (
  id_a INTEGER,
  name_a VARCHAR,
  value INTEGER
);
INSERT INTO sample_a (id_a, name_a, value) VALUES
  (10, 'A1', 5),
  (40, 'A2', 10),
  (80, 'A3', 15),
  (90, 'A4', 20)
;
CREATE OR REPLACE TABLE sample_b (
  id_b INTEGER,
  name_b VARCHAR,
  id_a INTEGER,
  value INTEGER
);
INSERT INTO sample_b (id_b, name_b, id_a, value) VALUES
  (4000, 'B1', 40, 10),
  (4001, 'B2', 10, 5),
  (9000, 'B3', 80, 15),
  (9099, 'B4', NULL, 200)
;
CREATE OR REPLACE TABLE sample_c (
  id_c INTEGER,
  name_c VARCHAR,
  id_a INTEGER,
  id_b INTEGER
);
INSERT INTO sample_c (id_c, name_c, id_a, id_b) VALUES
  (1012, 'C1', 10, NULL),
  (1040, 'C2', 40, 4000),
  (1041, 'C3', 40, 4001)
;