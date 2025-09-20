-- SQL para criar o esquema do banco de dados

-- Dropa o esquema para recriar do zero
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Tabela Product
CREATE TABLE product (
    asin VARCHAR(10) PRIMARY KEY,
    title VARCHAR(255),
    "group" VARCHAR(50),
    salesrank INTEGER,
    total_reviews INTEGER,
    downloaded INTEGER,
    avg_rating FLOAT
);

-- Tabela Customer
CREATE TABLE customer (
    customer_id VARCHAR(20) PRIMARY KEY
);

-- Tabela Category
CREATE TABLE category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100),
    parent_id INTEGER
);

-- Tabela Review
CREATE TABLE review (
    review_id SERIAL PRIMARY KEY,
    review_date DATE,
    rating INTEGER,
    helpful INTEGER,
    votes INTEGER,
    asin VARCHAR(10) REFERENCES product(asin),
    customer_id VARCHAR(20) REFERENCES customer(customer_id)
);

-- Tabela Product_Category
CREATE TABLE product_category (
    asin VARCHAR(10) REFERENCES product(asin),
    category_id INTEGER REFERENCES category(category_id),
    PRIMARY KEY (asin, category_id)
);

-- Tabela Product_Similar
CREATE TABLE product_similar (
    asin VARCHAR(10) REFERENCES product(asin),
    similar_asin VARCHAR(10) REFERENCES product(asin),
    PRIMARY KEY (asin, similar_asin)
);