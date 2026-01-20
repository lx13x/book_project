# 1_create_database.py - ИСПРАВЛЕННАЯ ВЕРСИЯ БЕЗ ОШИБОК
import sqlite3
import pandas as pd
import os
import re

print("="*60)
print("СОЗДАНИЕ БАЗЫ ДАННЫХ ДЛЯ КНИГ С ФИЛЬТРАЦИЕЙ")
print("="*60)

def clean_text(text):
    """Очищает текст от лишних символов"""
    if not text or str(text).lower() == 'nan' or text == 'None':
        return ""
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text

def is_valid_book(book_data):
    """Проверяет, является ли книга валидной"""
    title = clean_text(book_data.get('title', ''))
    author = clean_text(book_data.get('author', ''))
    price = book_data.get('price', 0)
    
    if not title or title == "Без названия" or title == "Название не указано":
        return False
    
    if price <= 0:
        return False
    
    if len(title) < 2:
        return False
    
    return True

# 1. Создаем базу данных
conn = sqlite3.connect('book_database.db')
cursor = conn.cursor()

# 2. Создаем таблицы - БЕЗ КОММЕНТАРИЕВ С #
cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    author TEXT,
    isbn TEXT UNIQUE,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS offers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    website TEXT NOT NULL,
    price REAL,
    url TEXT NOT NULL,
    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, website, url),
    FOREIGN KEY (product_id) REFERENCES products(id)
)
''')

print("✅ Созданы таблицы")

# 3. Загружаем и фильтруем данные
csv_files = [
    ('chitai_gorod_1000.csv', 'chitai-gorod'),
    ('labirint_1000.csv', 'labirint'),
    ('moscowbooks_1000.csv', 'moscowbooks')
]

all_books = []
valid_count = 0
invalid_count = 0

for filename, website in csv_files:
    if os.path.exists(filename):
        print(f"\n📖 Загружаем {filename}...")
        
        try:
            df = pd.read_csv(filename, encoding='utf-8-sig')
            print(f"   Найдено {len(df)} записей")
            
            # Берем все записи, а не только 200
            for _, row in df.iterrows():
                try:
                    book = {
                        'title': str(row.get('title', '')),
                        'author': str(row.get('author', '')),
                        'isbn': str(row.get('isbn', '')),
                        'price': float(row.get('price', 0)),
                        'website': website,
                        'url': str(row.get('url', '')),
                        'image_url': str(row.get('image_url', ''))
                    }
                    
                    # Фильтрация
                    if is_valid_book(book):
                        book['title'] = clean_text(book['title'])
                        book['author'] = clean_text(book['author'])
                        book['isbn'] = clean_text(book['isbn'])
                        
                        all_books.append(book)
                        valid_count += 1
                    else:
                        invalid_count += 1
                        
                except (ValueError, TypeError):
                    invalid_count += 1
                    continue
                    
            print(f"   ✅ Валидных: {valid_count}, ❌ Отброшено: {invalid_count}")
            valid_count = 0  # Сбрасываем для следующего файла
            invalid_count = 0
            
        except Exception as e:
            print(f"❌ Ошибка чтения файла: {str(e)[:50]}")
            continue
    else:
        print(f"⚠️ Файл {filename} не найден")

print(f"\n📚 Всего валидных книг для обработки: {len(all_books)}")

if len(all_books) == 0:
    print("\n❌ Нет валидных данных для обработки!")
    print("Проверьте CSV файлы и их содержимое")
    conn.close()
    exit()

# 4. Дедупликация
print("\n🔄 ВЫПОЛНЯЕМ ДЕДУПЛИКАЦИЮ...")

isbn_to_id = {}
title_author_to_id = {}
processed_urls = set()

products_added = 0
offers_added = 0
duplicate_offers_rejected = 0

for book in all_books:
    # Пропускаем если URL уже обработан
    url = book['url']
    if url in processed_urls:
        duplicate_offers_rejected += 1
        continue
    processed_urls.add(url)
    
    isbn = book['isbn']
    product_id = None
    
    # Способ 1: По ISBN
    if isbn and isbn != '':
        if isbn in isbn_to_id:
            product_id = isbn_to_id[isbn]
        else:
            # Проверяем по названию и автору
            title = book['title'].lower()
            author = book['author'].lower() if book['author'] else ''
            title_key = f"{title}_{author}"
            
            if title_key in title_author_to_id:
                product_id = title_author_to_id[title_key]
                isbn_to_id[isbn] = product_id
            else:
                # Создаем новую книгу
                cursor.execute('''
                    INSERT INTO products (title, author, isbn, image_url)
                    VALUES (?, ?, ?, ?)
                ''', (book['title'], book['author'], isbn, book['image_url']))
                
                product_id = cursor.lastrowid
                isbn_to_id[isbn] = product_id
                title_author_to_id[title_key] = product_id
                products_added += 1
    
    # Способ 2: По названию и автору
    if product_id is None:
        title = book['title'].lower()
        author = book['author'].lower() if book['author'] else ''
        title_key = f"{title}_{author}"
        
        if title_key in title_author_to_id:
            product_id = title_author_to_id[title_key]
        else:
            # Создаем новую книгу без ISBN
            cursor.execute('''
                INSERT INTO products (title, author, isbn, image_url)
                VALUES (?, ?, ?, ?)
            ''', (book['title'], book['author'], '', book['image_url']))
            
            product_id = cursor.lastrowid
            title_author_to_id[title_key] = product_id
            products_added += 1
    
    # Добавляем предложение
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO offers (product_id, website, price, url)
            VALUES (?, ?, ?, ?)
        ''', (product_id, book['website'], book['price'], book['url']))
        
        if cursor.rowcount > 0:
            offers_added += 1
        else:
            duplicate_offers_rejected += 1
            
    except Exception:
        duplicate_offers_rejected += 1
        continue

conn.commit()

# 5. Создаем индексы для производительности
print("\n📈 СОЗДАЕМ ИНДЕКСЫ...")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_isbn ON products(isbn)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_title ON products(title)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_offers_product_id ON offers(product_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_offers_price ON offers(price)")
print("✅ Индексы созданы")

# 6. Статистика
print("\n" + "="*60)
print("📊 РЕЗУЛЬТАТЫ ДЕДУПЛИКАЦИИ С ФИЛЬТРАЦИЕЙ")
print("="*60)

cursor.execute("SELECT COUNT(*) FROM products")
total_products = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM offers")
total_offers = cursor.fetchone()[0]

cursor.execute("SELECT website, COUNT(*) FROM offers GROUP BY website")
websites_stats = cursor.fetchall()

print(f"📚 Уникальных книг: {total_products}")
print(f"🛒 Уникальных предложений: {total_offers}")
print(f"🚫 Отклонено дубликатов предложений: {duplicate_offers_rejected}")

if total_products > 0:
    ratio = len(all_books) / total_products
    print(f"📈 Коэффициент дедупликации: {ratio:.2f}")

print("\n🌐 Предложений по сайтам:")
for website, count in websites_stats:
    print(f"   • {website}: {count}")

# 7. Примеры
print("\n📖 ПРИМЕРЫ КНИГ С НЕСКОЛЬКИМИ ПРЕДЛОЖЕНИЯМИ:")

cursor.execute('''
    SELECT p.title, p.author, COUNT(o.id) as offers_count,
           GROUP_CONCAT(DISTINCT o.website) as websites,
           MIN(o.price) as min_price, MAX(o.price) as max_price
    FROM products p
    JOIN offers o ON p.id = o.product_id
    GROUP BY p.id
    HAVING offers_count > 1
    ORDER BY offers_count DESC
    LIMIT 5
''')

examples = cursor.fetchall()

if examples:
    for i, (title, author, count, websites, min_price, max_price) in enumerate(examples, 1):
        short_title = title[:40] + "..." if len(title) > 40 else title
        print(f"\n{i}. {short_title}")
        if author and author != "Неизвестен":
            print(f"   Автор: {author}")
        print(f"   Предложений: {count} ({websites})")
        if min_price != max_price:
            print(f"   Цены: от {min_price}₽ до {max_price}₽")
        else:
            print(f"   Цена: {min_price}₽")
else:
    print("   ⚠️ Нет книг на нескольких сайтах")

# 8. Общая статистика цен
print("\n💰 ОБЩАЯ СТАТИСТИКА ПО ЦЕНАМ:")
cursor.execute('''
    SELECT 
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as avg_price,
        COUNT(*) as total_offers
    FROM offers 
    WHERE price > 0
''')

min_p, max_p, avg_p, total = cursor.fetchone()
print(f"   Минимальная цена: {min_p}₽")
print(f"   Максимальная цена: {max_p}₽")
print(f"   Средняя цена: {avg_p:.0f}₽")
print(f"   Всего предложений с ценой > 0: {total}")

conn.close()

print("\n" + "="*60)
print("🎉 БАЗА ДАННЫХ СОЗДАНА УСПЕШНО!")
print("="*60)
print("✅ Внедрены улучшения:")
print("   1. Фильтрация книг без названия/с ценой 0")
print("   2. Удаление дубликатов предложений")
print("   3. Улучшенная дедупликация")
print(f"📁 Файл базы: book_database.db")
print("\n🚀 Дальнейшие действия:")
print("   1. Проверить данные: python 2_check_data.py")
print("   2. Запустить сайт: python 3_website_final.py")