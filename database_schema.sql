-- database_schema.sql
-- Миграция базы данных для проекта "Интеграционная база данных книг"

-- Таблица raw_data - сырые данные из CSV
CREATE TABLE IF NOT EXISTS raw_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    title TEXT NOT NULL,
    author TEXT,
    isbn TEXT,
    price REAL,
    url TEXT NOT NULL,
    image_url TEXT,
    description TEXT,
    website TEXT NOT NULL,
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица products - уникальные книги после дедупликации
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    author TEXT,
    isbn TEXT UNIQUE,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица offers - предложения с сайтов
CREATE TABLE IF NOT EXISTS offers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    website TEXT NOT NULL,
    price REAL,
    url TEXT NOT NULL,
    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Индексы для ускорения поиска
CREATE INDEX IF NOT EXISTS idx_products_isbn ON products(isbn);
CREATE INDEX IF NOT EXISTS idx_products_title ON products(title);
CREATE INDEX IF NOT EXISTS idx_offers_product_id ON offers(product_id);
CREATE INDEX IF NOT EXISTS idx_offers_website ON offers(website);
CREATE INDEX IF NOT EXISTS idx_raw_data_isbn ON raw_data(isbn);

-- Триггер для обновления даты изменения
CREATE TRIGGER IF NOT EXISTS update_products_timestamp 
AFTER UPDATE ON products
BEGIN
    UPDATE products SET last_updated = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Комментарии к таблицам
COMMENT ON TABLE products IS 'Уникальные книги после дедупликации';
COMMENT ON TABLE offers IS 'Предложения книг с конкретных сайтов';
COMMENT ON TABLE raw_data IS 'Сырые данные из CSV файлов перед обработкой';