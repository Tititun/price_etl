CREATE TABLE IF NOT EXISTS supermarkets (
    supermarket_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS categories (
	supermarket_id INT NOT NULL,
    category_id VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    FOREIGN KEY categories_supermarket (supermarket_id)
				REFERENCES supermarkets (supermarket_id) ON DELETE CASCADE ON UPDATE CASCADE,
	PRIMARY KEY (supermarket_id, category_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(30) NOT NULL,
    supermarket_id INTEGER NOT NULL,
    category_id VARCHAR(100) NOT NULL,
    name VARCHAR(200) NOT NULL,
    created_on DATE DEFAULT (CURRENT_DATE),
    FOREIGN KEY product_supermarket_category (supermarket_id, category_id)
        REFERENCES categories (supermarket_id, category_id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (product_id, supermarket_id)
);


CREATE TABLE IF NOT EXISTS product_info (
    product_id VARCHAR(30) NOT NULL,
    observed_on DATE DEFAULT (CURRENT_DATE),
    price DECIMAL(8, 2),
    discounted_price DECIMAL(8, 2),
    rating DECIMAL (3, 2),
    rates_count MEDIUMINT UNSIGNED,
    unit VARCHAR(100),
    FOREIGN KEY product_info_product (product_id) REFERENCES products (product_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT product_observed_date_unique UNIQUE (product_id, observed_on)
)