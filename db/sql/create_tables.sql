CREATE TABLE IF NOT EXISTS supermarkets (
    supermarket_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
	supermarket_id INT NOT NULL,
    category_code VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    last_scraped_on DATE,
    FOREIGN KEY categories_supermarket (supermarket_id)
				REFERENCES supermarkets (supermarket_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT supermarket_category_code_uniques UNIQUE (supermarket_id, category_code)
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_code VARCHAR(30) NOT NULL,
    category_id INT NOT NULL,
    name VARCHAR(200) NOT NULL,
    url VARCHAR(200) NOT NULL,
    created_on DATE DEFAULT (CURRENT_DATE),
    FOREIGN KEY product_category (category_id)
        REFERENCES categories (category_id) ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE TABLE IF NOT EXISTS product_info (
    product_id INT NOT NULL,
    observed_on DATE DEFAULT (CURRENT_DATE),
    price DECIMAL(8, 2),
    discounted_price DECIMAL(8, 2),
    rating DECIMAL (3, 2),
    rates_count MEDIUMINT UNSIGNED,
    unit VARCHAR(100),
    FOREIGN KEY product_info_product (product_id)
        REFERENCES products (product_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (product_id, observed_on)
)