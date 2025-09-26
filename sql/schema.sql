-- Criação do esquema do banco de dados (TP1-BD)
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Tabela Product: informações básicas do produto
CREATE TABLE product (
    asin VARCHAR(20) PRIMARY KEY,
    title TEXT NOT NULL,
    group_name TEXT,               -- grupo principal do produto (ex: Book, DVD)
    salesrank INTEGER,
    total_reviews INTEGER,
    downloaded INTEGER,
    avg_rating FLOAT
);

-- Tabela Customer: clientes identificados por ID
CREATE TABLE customer (
    customer_id VARCHAR(20) PRIMARY KEY
    -- (Nenhum outro atributo disponível no dataset)
);

-- Tabela Category: categorias de produto (hierarquia)
CREATE TABLE category (
    category_id INTEGER PRIMARY KEY,              -- usando IDs fornecidos no dataset
    category_name TEXT NOT NULL,
    parent_id INTEGER,
    CONSTRAINT fk_category_parent
        FOREIGN KEY (parent_id)
        REFERENCES category (category_id)
        DEFERRABLE INITIALLY DEFERRED
);

-- Tabela Review: avaliações de produtos pelos clientes
CREATE TABLE review (
    review_id SERIAL PRIMARY KEY,
    review_date DATE NOT NULL,
    rating INTEGER NOT NULL,
    helpful INTEGER NOT NULL,
    votes INTEGER NOT NULL,
    asin VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    CONSTRAINT fk_review_product
        FOREIGN KEY (asin)
        REFERENCES product (asin)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_review_customer
        FOREIGN KEY (customer_id)
        REFERENCES customer (customer_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (asin, customer_id, review_date)
);

-- Tabela Product_Category: relação N:N entre Product e Category (categoria(s) por produto)
CREATE TABLE product_category (
    asin VARCHAR(10) NOT NULL,
    category_id INTEGER NOT NULL,
    PRIMARY KEY (asin, category_id),
    CONSTRAINT fk_product_category_product
        FOREIGN KEY (asin)
        REFERENCES product (asin)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_product_category_category
        FOREIGN KEY (category_id)
        REFERENCES category (category_id)
        DEFERRABLE INITIALLY DEFERRED
);

-- Tabela Product_Similar: relação N:N de "produtos similares" (co-compra)

CREATE TABLE product_similar (
    asin VARCHAR(20) NOT NULL,
    similar_asin VARCHAR(20) NOT NULL,
    PRIMARY KEY (asin, similar_asin),
    CONSTRAINT fk_product_similar_product
        FOREIGN KEY (asin)
        REFERENCES product (asin)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_product_similar_similar
        FOREIGN KEY (similar_asin)
        REFERENCES product (asin)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_review_asin ON review (asin);
CREATE INDEX idx_product_category_cat ON product_category (category_id);
