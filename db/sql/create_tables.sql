CREATE TABLE IF NOT EXISTS supermarkets (
    supermarket_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS categories (
	category_id INT AUTO_INCREMENT PRIMARY KEY,
	supermarket_id INT NOT NULL,
    inner_code VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    FOREIGN KEY categories_supermarket (supermarket_id)
				REFERENCES supermarkets (supermarket_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT supermarket_id_inner_code_unique UNIQUE (supermarket_id, inner_code)
);

CREATE TABLE IF NOT EXISTS products (
	product_id INTEGER AUTO_INCREMENT PRIMARY KEY,
    category_id INTEGER NOT NULL,
    inner_code VARCHAR(30) NOT NULL,
    name VARCHAR(200) NOT NULL,
    created_on DATE DEFAULT (CURRENT_DATE),
    FOREIGN KEY product_category (category_id) REFERENCES categories (category_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT category_product_unique UNIQUE (category_id, inner_code)
);


CREATE TABLE IF NOT EXISTS product_info (
    product_id INTEGER NOT NULL,
    observed_on DATE DEFAULT (CURRENT_DATE),
    price DECIMAL(8, 2),
    discounted_price DECIMAL(8, 2),
    rating DECIMAL (3, 2),
    rates_count MEDIUMINT UNSIGNED,
    unit VARCHAR(100),
    FOREIGN KEY product_info_product (product_id) REFERENCES products (product_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT product_observed_date_unique UNIQUE (product_id, observed_on)
);