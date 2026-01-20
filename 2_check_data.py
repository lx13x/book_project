# 2_check_data.py - ПРОВЕРКА ДАННЫХ
import sqlite3
import os

print("="*60)
print("ПРОВЕРКА БАЗЫ ДАННЫХ КНИГ")
print("="*60)

def check_database():
    # Проверяем, есть ли файл базы
    if not os.path.exists('book_database.db'):
        print("❌ Файл book_database.db не найден!")
        print("🔧 Сначала запустите: python 1_create_database.py")
        return
    
    conn = sqlite3.connect('book_database.db')
    cursor = conn.cursor()
    
    print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
    print("-"*40)
    
    # 1. Основные цифры
    cursor.execute("SELECT COUNT(*) FROM products")
    products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM offers")
    offers = cursor.fetchone()[0]
    
    print(f"📚 Уникальных книг: {products}")
    print(f"🛒 Предложений: {offers}")
    print(f"📈 Среднее предложений на книгу: {offers/products:.1f}")
    
    # 2. По сайтам
    print("\n🌐 ПО САЙТАМ:")
    cursor.execute('''
        SELECT website, COUNT(*) as count, 
               AVG(price) as avg_price,
               MIN(price) as min_price,
               MAX(price) as max_price
        FROM offers 
        GROUP BY website
    ''')
    
    for site, count, avg, min_p, max_p in cursor.fetchall():
        print(f"  • {site}: {count} предложений")
        print(f"    💰 Цены: от {min_p}₽ до {max_p}₽ (средняя: {avg:.0f}₽)")
    
    # 3. Книги на нескольких сайтах
    print("\n🏆 КНИГИ НА НЕСКОЛЬКИХ САЙТАХ:")
    cursor.execute('''
        SELECT p.title, p.author, 
               COUNT(DISTINCT o.website) as sites_count,
               GROUP_CONCAT(DISTINCT o.website) as websites
        FROM products p
        JOIN offers o ON p.id = o.product_id
        GROUP BY p.id
        HAVING sites_count > 1
        ORDER BY sites_count DESC
        LIMIT 10
    ''')
    
    multi_site = cursor.fetchall()
    
    if multi_site:
        for title, author, count, websites in multi_site:
            short_title = title[:35] + "..." if len(title) > 35 else title
            print(f"  📖 {short_title}")
            print(f"    👤 {author}")
            print(f"    🌐 {count} сайтов: {websites}")
            print()
    else:
        print("  ⚠️ Нет книг на нескольких сайтах")
    
    # 4. Случайные книги для примера
    print("🎲 СЛУЧАЙНЫЕ КНИГИ ИЗ БАЗЫ:")
    cursor.execute('''
        SELECT p.title, p.author, 
               COUNT(o.id) as offers_count,
               MIN(o.price) as min_price
        FROM products p
        LEFT JOIN offers o ON p.id = o.product_id
        GROUP BY p.id
        ORDER BY RANDOM()
        LIMIT 5
    ''')
    
    for title, author, count, min_price in cursor.fetchall():
        short_title = title[:30] + "..." if len(title) > 30 else title
        print(f"  • {short_title}")
        print(f"    Автор: {author}, Предложений: {count}, Цена от: {min_price}₽")
    
    conn.close()
    
    print("\n" + "="*60)
    print("✅ ПРОВЕРКА ЗАВЕРШЕНА")
    print("="*60)

if __name__ == "__main__":
    check_database()