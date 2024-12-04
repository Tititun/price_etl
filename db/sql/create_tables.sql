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