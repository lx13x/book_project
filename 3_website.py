# 3_website.py
import http.server
import socketserver
import sqlite3
import urllib.parse
from datetime import datetime
import os
import sys
import json

print("="*60)
print("🚀 ЗАПУСК БИБЛИОТЕКИ КНИГ")
print("="*60)

# Проверяем базу данных
if not os.path.exists('book_database.db'):
    print("❌ ОШИБКА: Файл book_database.db не найден!")
    print("\n🔧 РЕШЕНИЕ:")
    print("1. Сначала запустите: python 1_create_database.py")
    print("2. Убедитесь, что CSV файлы в той же папке")
    print("\n📁 Текущая папка:", os.getcwd())
    print("="*60)
    input("Нажмите Enter для выхода...")
    sys.exit(1)

def get_database_stats():
    """Получает статистику из базы данных"""
    conn = sqlite3.connect('book_database.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM products WHERE title != '' AND title IS NOT NULL")
    total_books = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM offers WHERE price > 0")
    total_offers = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(price) FROM offers WHERE price > 0")
    avg_price = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(DISTINCT website) FROM offers")
    websites = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_books': total_books,
        'total_offers': total_offers,
        'avg_price': round(avg_price),
        'websites': websites
    }

def get_all_websites():
    """Получает список всех магазинов"""
    conn = sqlite3.connect('book_database.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT website FROM offers ORDER BY website")
    websites = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return websites

def search_books(query="", sort_by="relevance", website_filter="all", min_price=None, max_price=None):
    """Ищет книги в базе данных с фильтрами"""
    conn = sqlite3.connect('book_database.db')
    cursor = conn.cursor()
    
    # Базовый SQL
    select_sql = '''
        SELECT p.id, p.title, p.author, p.image_url,
               MIN(o.price) as min_price,
               MAX(o.price) as max_price,
               GROUP_CONCAT(DISTINCT o.website) as websites,
               COUNT(o.id) as offers_count
        FROM products p
        JOIN offers o ON p.id = o.product_id
    '''
    
    where_conditions = ["p.title != '' AND p.title IS NOT NULL", "o.price > 0"]
    params = []
    
    # Поисковый запрос
    if query:
        words = query.strip().split()
        if words:
            search_conditions = []
            for word in words:
                search_conditions.append("(p.title LIKE ? OR p.author LIKE ?)")
                params.extend([f'%{word}%', f'%{word}%'])
            where_conditions.append(f"({' OR '.join(search_conditions)})")
    
    # Фильтр по магазину
    if website_filter != "all":
        where_conditions.append("o.website = ?")
        params.append(website_filter)
    
    # Фильтр по цене
    if min_price is not None:
        where_conditions.append("o.price >= ?")
        params.append(float(min_price))
    
    if max_price is not None:
        where_conditions.append("o.price <= ?")
        params.append(float(max_price))
    
    # Собираем WHERE
    where_sql = " AND ".join(where_conditions)
    
    # Сортировка
    order_by = "offers_count DESC"
    if sort_by == "price_asc":
        order_by = "min_price ASC"
    elif sort_by == "price_desc":
        order_by = "min_price DESC"
    elif sort_by == "title":
        order_by = "p.title ASC"
    elif sort_by == "author":
        order_by = "p.author ASC"
    
    # Финальный SQL
    sql = f'''
        {select_sql}
        WHERE {where_sql}
        GROUP BY p.id
        ORDER BY {order_by}
        LIMIT 100
    '''
    
    cursor.execute(sql, params)
    
    books = []
    for row in cursor.fetchall():
        book_id, title, author, image_url, min_price_val, max_price_val, websites, offers_count = row
        
        # Получаем все предложения для книги
        cursor2 = conn.cursor()
        cursor2.execute('''
            SELECT website, price, url 
            FROM offers 
            WHERE product_id = ? AND price > 0
            ORDER BY price
        ''', (book_id,))
        
        offers = []
        for website, price, url in cursor2.fetchall():
            offers.append({
                'website': website,
                'price': price,
                'url': url
            })
        
        if not offers:
            cursor2.close()
            continue
        
        books.append({
            'id': book_id,
            'title': title,
            'author': author or 'Неизвестен',
            'image_url': image_url or '',
            'min_price': min_price_val,
            'max_price': max_price_val,
            'offers_count': offers_count,
            'websites': websites.split(',') if websites else [],
            'offers': offers
        })
        
        cursor2.close()
    
    conn.close()
    return books

def get_book_details(book_id):
    """Получает полную информацию о книге"""
    conn = sqlite3.connect('book_database.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT p.title, p.author, p.image_url, p.created_at
        FROM products p
        WHERE p.id = ?
    ''', (book_id,))
    
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None
    
    title, author, image_url, created_at = row
    
    # Получаем все предложения
    cursor.execute('''
        SELECT website, price, url 
        FROM offers 
        WHERE product_id = ? AND price > 0
        ORDER BY price
    ''', (book_id,))
    
    offers = []
    for website, price, url in cursor.fetchall():
        offers.append({
            'website': website,
            'price': price,
            'url': url
        })
    
    # Статистика по книге
    cursor.execute('''
        SELECT 
            COUNT(DISTINCT website) as websites_count,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price
        FROM offers 
        WHERE product_id = ? AND price > 0
    ''', (book_id,))
    
    stats_row = cursor.fetchone()
    
    book_details = {
        'id': book_id,
        'title': title,
        'author': author or 'Неизвестен',
        'image_url': image_url or '',
        'created_at': created_at,
        'offers': offers,
        'stats': {
            'websites_count': stats_row[0] if stats_row else 0,
            'min_price': stats_row[1] if stats_row else 0,
            'max_price': stats_row[2] if stats_row else 0,
            'avg_price': stats_row[3] if stats_row else 0
        }
    }
    
    conn.close()
    return book_details

class BookWebsiteHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Обрабатываем разные маршруты
        if self.path.startswith('/api/book/'):
            # API для получения информации о книге
            book_id = self.path.split('/')[-1]
            if book_id.isdigit():
                book_details = get_book_details(int(book_id))
                if book_details:
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(book_details).encode('utf-8'))
                else:
                    self.send_error(404, "Книга не найдена")
            else:
                self.send_error(400, "Некорректный ID книги")
            return
        
        # Главная страница
        elif self.path == '/' or '?' in self.path or self.path == '/index.html':
            # Извлекаем параметры
            parsed_url = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed_url.query)
            
            search_query = params.get('q', [''])[0]
            sort_by = params.get('sort', ['relevance'])[0]
            website_filter = params.get('website', ['all'])[0]
            min_price = params.get('min_price', [None])[0]
            max_price = params.get('max_price', [None])[0]
            
            # Получаем данные
            stats = get_database_stats()
            websites = get_all_websites()
            books = search_books(search_query, sort_by, website_filter, min_price, max_price)
            current_time = datetime.now().strftime('%d.%m.%Y %H:%M')
            
            # Генерируем HTML
            html = self.generate_main_page(stats, books, search_query, sort_by, 
                                         website_filter, websites, min_price, 
                                         max_price, current_time)
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
        else:
            super().do_GET()
    
    def generate_main_page(self, stats, books, search_query, sort_by, 
                          website_filter, websites, min_price, max_price, current_time):
        """Генерирует главную страницу"""
        
        # Основной HTML
        html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Библиотека книг</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>📚</text></svg>">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1600px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        header {
            background: linear-gradient(135deg, #4a5568 0%, #2d3748 100%);
            color: white;
            padding: 30px 40px;
        }
        .header-content {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
        }
        .site-title {
            font-size: 2.2em;
            font-weight: 600;
            text-decoration: none;
            color: white;
            transition: color 0.3s;
        }
        .site-title:hover {
            color: #cbd5e0;
        }
        .header-stats {
            text-align: right;
            font-size: 1.1em;
            opacity: 0.9;
        }
        .header-stat-item {
            margin-bottom: 5px;
        }
        
        /* Главный поиск */
        .main-search-container {
            background: white;
            padding: 40px;
            text-align: center;
            border-bottom: 2px solid #e9ecef;
        }
        .main-search-form {
            display: flex;
            gap: 15px;
            max-width: 800px;
            margin: 0 auto;
        }
        .main-search-input {
            flex: 1;
            padding: 16px 20px;
            border: 2px solid #dee2e6;
            border-radius: 10px;
            font-size: 18px;
            transition: all 0.3s;
        }
        .main-search-input:focus {
            border-color: #667eea;
            outline: none;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.2);
        }
        .main-search-btn {
            padding: 0 40px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 18px;
            cursor: pointer;
            transition: all 0.3s;
            white-space: nowrap;
        }
        .main-search-btn:hover {
            background: #764ba2;
            transform: translateY(-2px);
        }
        
        /* Фильтры */
        .filters {
            background: #f8f9fa;
            padding: 25px 40px;
            border-bottom: 2px solid #e9ecef;
        }
        .filter-label {
            display: block;
            margin-bottom: 8px;
            font-weight: bold;
            color: #495057;
        }
        .filter-row {
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            align-items: flex-end;
        }
        .filter-item {
            flex: 1;
            min-width: 200px;
        }
        .select-filter, .price-input {
            width: 100%;
            padding: 12px;
            border: 2px solid #dee2e6;
            border-radius: 8px;
            font-size: 16px;
            background: white;
        }
        .select-filter:focus, .price-input:focus {
            border-color: #667eea;
            outline: none;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        .filter-buttons {
            display: flex;
            gap: 15px;
            align-items: center;
        }
        .filter-btn {
            background: #667eea;
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 8px;
            font-size: 16px;
            cursor: pointer;
            transition: all 0.3s;
            white-space: nowrap;
            height: 44px;
        }
        .filter-btn:hover {
            background: #764ba2;
            transform: translateY(-2px);
        }
        .clear-btn {
            background: #6c757d;
        }
        .clear-btn:hover {
            background: #5a6268;
        }
        
        /* Статистика */
        .stats {
            display: flex;
            justify-content: space-around;
            background: #f7fafc;
            padding: 20px;
            flex-wrap: wrap;
            border-bottom: 2px solid #e9ecef;
        }
        .stat-card {
            text-align: center;
            padding: 15px;
            min-width: 200px;
        }
        .stat-number {
            font-size: 2.5em;
            color: #667eea;
            font-weight: bold;
        }
        .stat-label {
            color: #6c757d;
            font-size: 0.95em;
            margin-top: 5px;
        }
        
        /* Книги */
        .books-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 30px;
            padding: 40px;
        }
        .book-card {
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 8px 25px rgba(0,0,0,0.08);
            transition: all 0.3s ease;
            border: 1px solid #e9ecef;
            cursor: pointer;
            position: relative;
        }
        .book-card:hover {
            transform: translateY(-8px);
            box-shadow: 0 20px 40px rgba(0,0,0,0.15);
            border-color: #667eea;
        }
        .book-image-container {
            width: 100%;
            height: 373px; /* Соотношение 3:4 (280 * 4/3 = 373) */
            overflow: hidden;
            position: relative;
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        }
        .book-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
            transition: transform 0.5s;
        }
        .book-card:hover .book-image {
            transform: scale(1.05);
        }
        .no-image {
            width: 100%;
            height: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 4em;
        }
        .book-info {
            padding: 20px;
        }
        .book-title {
            font-size: 1.1em;
            font-weight: 600;
            margin-bottom: 10px;
            color: #2d3748;
            line-height: 1.4;
            height: 3em;
            overflow: hidden;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
        }
        .book-author {
            color: #6c757d;
            margin-bottom: 12px;
            font-style: italic;
            font-size: 0.95em;
        }
        .book-price {
            font-size: 1.4em;
            color: #28a745;
            font-weight: bold;
            margin-bottom: 8px;
        }
        .price-range {
            color: #6c757d;
            font-size: 0.9em;
            margin-bottom: 15px;
        }
        .website-badge {
            background: #e9ecef;
            color: #495057;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            display: inline-block;
            margin-right: 6px;
            margin-bottom: 6px;
            border: 1px solid #dee2e6;
        }
        
        /* Модальное окно */
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .modal.show {
            display: flex;
            opacity: 1;
            align-items: center;
            justify-content: center;
        }
        .modal-content {
            background: white;
            border-radius: 16px;
            width: 90%;
            max-width: 1000px;
            max-height: 90vh;
            overflow-y: auto;
            box-shadow: 0 25px 50px rgba(0,0,0,0.25);
            animation: modalSlideIn 0.3s ease-out;
        }
        @keyframes modalSlideIn {
            from { transform: translateY(-50px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }
        .modal-header {
            padding: 25px 30px;
            border-bottom: 1px solid #e9ecef;
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        }
        .modal-title {
            font-size: 1.5em;
            font-weight: 600;
            color: #2d3748;
        }
        .modal-close {
            background: none;
            border: none;
            font-size: 1.8em;
            cursor: pointer;
            color: #6c757d;
            padding: 0;
            width: 40px;
            height: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 50%;
            transition: all 0.2s;
        }
        .modal-close:hover {
            background: #e9ecef;
            color: #495057;
        }
        .modal-body {
            padding: 30px;
            display: grid;
            grid-template-columns: 300px 1fr;
            gap: 40px;
        }
        .modal-image-container {
            width: 300px;
            height: 400px; /* Соотношение 3:4 (300 * 4/3 = 400) */
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        }
        .modal-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
        }
        .modal-details h3 {
            margin: 0 0 20px 0;
            font-size: 1.8em;
            color: #2d3748;
            line-height: 1.3;
        }
        .modal-author {
            color: #6c757d;
            font-size: 1.1em;
            margin-bottom: 25px;
            font-style: italic;
        }
        .detail-item {
            margin-bottom: 15px;
            display: flex;
        }
        .detail-label {
            min-width: 120px;
            font-weight: 600;
            color: #495057;
        }
        .detail-value {
            color: #2d3748;
            flex: 1;
        }
        .offers-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 25px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            border-radius: 8px;
            overflow: hidden;
        }
        .offers-table th {
            background: #f8f9fa;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            color: #495057;
            border-bottom: 2px solid #e9ecef;
        }
        .offers-table td {
            padding: 15px;
            border-bottom: 1px solid #e9ecef;
        }
        .offers-table tr:hover {
            background: #f8f9fa;
        }
        .buy-btn-modal {
            background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
            color: white;
            border: none;
            padding: 14px 28px;
            border-radius: 8px;
            font-size: 1.1em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            display: inline-flex;
            align-items: center;
            gap: 10px;
            text-decoration: none;
            margin-top: 20px;
        }
        .buy-btn-modal:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 20px rgba(40, 167, 69, 0.2);
        }
        
        /* Сообщения */
        .no-books {
            text-align: center;
            padding: 60px;
            color: #6c757d;
            font-size: 1.2em;
            grid-column: 1 / -1;
        }
        .search-info {
            text-align: center;
            padding: 15px;
            color: #495057;
            font-size: 1.1em;
            background: #f8f9fa;
            border-radius: 8px;
            margin: 0 40px 20px 40px;
        }
        
        /* Футер */
        .footer {
            text-align: center;
            padding: 25px;
            background: #4a5568;
            color: white;
            margin-top: 30px;
        }
        .footer-links {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }
        .footer-link {
            color: #cbd5e0;
            text-decoration: none;
            transition: color 0.3s;
        }
        .footer-link:hover {
            color: white;
        }
        
        /* Адаптивность */
        @media (max-width: 768px) {
            .modal-body {
                grid-template-columns: 1fr;
                gap: 25px;
            }
            .modal-image-container {
                width: 100%;
                height: 300px;
            }
            .filter-row {
                flex-direction: column;
            }
            .filter-item {
                min-width: 100%;
            }
            .filter-buttons {
                width: 100%;
                justify-content: center;
            }
            .header-content {
                flex-direction: column;
                gap: 10px;
                text-align: center;
            }
            .header-stats {
                text-align: center;
            }
            .main-search-form {
                flex-direction: column;
            }
            .books-grid {
                grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
                padding: 20px;
            }
            .book-image-container {
                height: 333px; /* 250 * 4/3 = 333 */
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-content">
                <a href="/" class="site-title">Библиотека книг</a>
                <div class="header-stats">
                    <div class="header-stat-item">Всего книг: ''' + str(stats['total_books']) + '''</div>
                    <div class="header-stat-item">Предложений: ''' + str(stats['total_offers']) + '''</div>
                    <div class="header-stat-item">Средняя цена: ''' + str(stats['avg_price']) + '''₽</div>
                </div>
            </div>
        </header>
        
        <!-- Главный поиск -->
        <div class="main-search-container">
            <form method="GET" action="/" class="main-search-form">
                <input type="text" name="q" class="main-search-input" 
                       placeholder="Введите название книги или автора..." 
                       value="''' + search_query + '''">
                <button type="submit" class="main-search-btn">Найти</button>
            </form>
            <p style="margin-top: 15px; color: #6c757d; font-size: 0.95em;">
                Поиск по ''' + str(stats['total_books']) + ''' книгам из ''' + str(len(websites)) + ''' магазинов
            </p>
        </div>
        
        <!-- Фильтры -->
        <div class="filters">
            <form method="GET" action="/" id="filterForm">
                <!-- Скрытое поле для поискового запроса -->
                <input type="hidden" name="q" value="''' + search_query + '''">
                
                <div class="filter-row">
                    <div class="filter-item">
                        <div class="filter-label">Сортировка</div>
                        <select name="sort" class="select-filter">
                            <option value="relevance" ''' + ("selected" if sort_by == "relevance" else "") + '''>По релевантности</option>
                            <option value="price_asc" ''' + ("selected" if sort_by == "price_asc" else "") + '''>Цена (дешевле)</option>
                            <option value="price_desc" ''' + ("selected" if sort_by == "price_desc" else "") + '''>Цена (дороже)</option>
                            <option value="title" ''' + ("selected" if sort_by == "title" else "") + '''>По названию (А-Я)</option>
                            <option value="author" ''' + ("selected" if sort_by == "author" else "") + '''>По автору (А-Я)</option>
                        </select>
                    </div>
                    
                    <div class="filter-item">
                        <div class="filter-label">Магазин</div>
                        <select name="website" class="select-filter">
                            <option value="all" ''' + ("selected" if website_filter == "all" else "") + '''>Все магазины</option>'''
        
        # Добавляем варианты магазинов
        for website in websites:
            selected = "selected" if website_filter == website else ""
            html += '''                            <option value="''' + website + '''" ''' + selected + '''>''' + website + '''</option>
'''
        
        html += '''                        </select>
                    </div>
                    
                    <div class="filter-item">
                        <div class="filter-label">Цена от</div>
                        <input type="number" name="min_price" class="price-input" 
                               placeholder="0" min="0" step="10" value="''' + (min_price or '') + '''">
                    </div>
                    
                    <div class="filter-item">
                        <div class="filter-label">Цена до</div>
                        <input type="number" name="max_price" class="price-input" 
                               placeholder="10000" min="0" step="10" value="''' + (max_price or '') + '''">
                    </div>
                    
                    <div class="filter-buttons">
                        <button type="submit" class="filter-btn">Применить фильтры</button>
                        <a href="/" class="filter-btn clear-btn">Сбросить всё</a>
                    </div>
                </div>
            </form>
        </div>
        
        <!-- Статистика -->
        <div class="stats">
            <div class="stat-card">
                <div class="stat-number">''' + str(stats['total_books']) + '''</div>
                <div class="stat-label">Уникальных книг</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">''' + str(stats['total_offers']) + '''</div>
                <div class="stat-label">Предложений</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">''' + str(stats['avg_price']) + '''₽</div>
                <div class="stat-label">Средняя цена</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">''' + str(stats['websites']) + '''</div>
                <div class="stat-label">Магазинов</div>
            </div>
        </div>'''
        
        # Информация о поиске
        if search_query or website_filter != "all" or min_price or max_price:
            filter_text = []
            if search_query:
                filter_text.append('поиск: "' + search_query + '"')
            if website_filter != "all":
                filter_text.append('магазин: ' + website_filter)
            if min_price:
                filter_text.append('цена от: ' + min_price + '₽')
            if max_price:
                filter_text.append('цена до: ' + max_price + '₽')
            
            html += '''
        <div class="search-info">
            🔍 Найдено ''' + str(len(books)) + ''' книг (''' + ', '.join(filter_text) + ''')
        </div>'''
        
        html += '''
        <!-- Книги -->
        <div class="books-grid">'''
        
        # Добавляем книги
        if books:
            for book in books:
                # Изображение
                if book['image_url'] and book['image_url'] != '':
                    image_html = '<img src="' + book['image_url'] + '" alt="' + book['title'] + '" class="book-image">'
                else:
                    image_html = '<div class="no-image">📖</div>'
                
                # Автор
                author = book['author'] if book['author'] and book['author'] != 'Неизвестен' else 'Автор не указан'
                
                # Сайты
                websites_html = ''
                unique_websites = set(book['websites'])
                for website in unique_websites:
                    websites_html += '<span class="website-badge">' + website + '</span> '
                
                # Цены
                price_html = '<div class="book-price">от ' + str(book['min_price']) + '₽</div>'
                if book['min_price'] != book['max_price'] and len(book['offers']) > 1:
                    price_html += '<div class="price-range">до ' + str(book['max_price']) + '₽</div>'
                
                # Обрезаем длинное название
                display_title = book['title']
                if len(display_title) > 80:
                    display_title = display_title[:77] + '...'
                
                html += '''
            <div class="book-card" onclick="showBookModal(''' + str(book['id']) + ''')">
                <div class="book-image-container">
                    ''' + image_html + '''
                </div>
                <div class="book-info">
                    <div class="book-title" title="''' + book['title'].replace('"', '&quot;') + '''">''' + display_title + '''</div>
                    <div class="book-author">''' + author + '''</div>
                    ''' + price_html + '''
                    <div style="margin-top: 10px;">
                        ''' + websites_html + '''
                    </div>
                </div>
            </div>'''
        else:
            html += '''
            <div class="no-books">
                <h3>😔 Книги не найдены</h3>
                <p>Попробуйте изменить параметры поиска или сбросить фильтры</p>
            </div>'''
        
        # Завершаем основную часть
        html += '''
        </div>
        
        <!-- Футер -->
        <div class="footer">
            <div class="footer-links">
                <a href="/" class="footer-link">Главная</a>
                <a href="/?sort=price_asc" class="footer-link">Дешевые книги</a>
                <a href="/?sort=price_desc" class="footer-link">Дорогие книги</a>
                <a href="/?sort=title" class="footer-link">По алфавиту</a>
            </div>
            <p>Библиотека книг • Сравнение цен из разных магазинов • Только актуальные предложения</p>
            <p style="margin-top: 15px; color: #cbd5e0; font-size: 0.9em;">''' + current_time + ''' • Данные обновлены автоматически</p>
        </div>
    </div>
    
    <!-- Модальное окно -->
    <div class="modal" id="bookModal">
        <div class="modal-content">
            <div class="modal-header">
                <div class="modal-title">Детали книги</div>
                <button class="modal-close" onclick="hideBookModal()">×</button>
            </div>
            <div class="modal-body" id="modalBody">
                <!-- Заполняется JavaScript -->
            </div>
        </div>
    </div>
    
    <script>
        let currentBookId = null;
        
        // Показать модальное окно
        function showBookModal(bookId) {
            currentBookId = bookId;
            const modal = document.getElementById('bookModal');
            const modalBody = document.getElementById('modalBody');
            
            // Показываем загрузку
            modalBody.innerHTML = '<div style="padding: 40px; text-align: center;">Загрузка...</div>';
            modal.classList.add('show');
            
            // Загружаем данные
            fetch("/api/book/" + bookId)
                .then(response => response.json())
                .then(book => {
                    renderBookModal(book);
                })
                .catch(error => {
                    modalBody.innerHTML = `
                        <div style="padding: 40px; text-align: center; color: #dc3545;">
                            <h3>Ошибка загрузки</h3>
                            <p>Не удалось загрузить информацию о книге</p>
                        </div>
                    `;
                });
        }
        
        // Скрыть модальное окно
        function hideBookModal() {
            document.getElementById('bookModal').classList.remove('show');
        }
        
        // Закрыть по клику вне окна
        document.getElementById('bookModal').addEventListener('click', function(e) {
            if (e.target === this) {
                hideBookModal();
            }
        });
        
        // Рендер модального окна
        function renderBookModal(book) {
            const modalBody = document.getElementById('modalBody');
            
            // Изображение
            const imageHtml = book.image_url 
                ? `<img src="${book.image_url}" alt="${book.title}" class="modal-image">`
                : `<div class="no-image">📖</div>`;
            
            // Предложения
            let offersHtml = '';
            if (book.offers && book.offers.length > 0) {
                offersHtml = `
                    <table class="offers-table">
                        <thead>
                            <tr>
                                <th>Магазин</th>
                                <th>Цена</th>
                                <th>Действие</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                book.offers.forEach(offer => {
                    offersHtml += `
                        <tr>
                            <td>${offer.website}</td>
                            <td><strong>${offer.price}₽</strong></td>
                            <td><a href="${offer.url}" target="_blank" class="buy-btn-modal">Купить</a></td>
                        </tr>
                    `;
                });
                
                offersHtml += `</tbody></table>`;
            }
            
            // Дата
            const date = new Date(book.created_at);
            const formattedDate = date.toLocaleDateString('ru-RU');
            
            // HTML
            modalBody.innerHTML = `
                <div class="modal-image-container">
                    ${imageHtml}
                </div>
                <div class="modal-details">
                    <h3>${book.title}</h3>
                    <div class="modal-author">${book.author}</div>
                    
                    <div class="detail-item">
                        <div class="detail-label">Дата обновления:</div>
                        <div class="detail-value">${formattedDate}</div>
                    </div>
                    
                    <div class="detail-item">
                        <div class="detail-label">Магазинов:</div>
                        <div class="detail-value">${book.stats.websites_count}</div>
                    </div>
                    
                    <div class="detail-item">
                        <div class="detail-label">Диапазон цен:</div>
                        <div class="detail-value">
                            ${book.stats.min_price}₽ — ${book.stats.max_price}₽ 
                            (средняя: ${Math.round(book.stats.avg_price)}₽)
                        </div>
                    </div>
                    
                    <div style="margin-top: 30px;">
                        <h4>Где купить:</h4>
                        ${offersHtml}
                    </div>
                    
                    ${book.offers && book.offers.length > 0 ? `
                    <a href="${book.offers[0].url}" target="_blank" class="buy-btn-modal">
                        Купить за ${book.offers[0].price}₽
                    </a>
                    ` : ''}
                </div>
            `;
        }
        
        // Закрытие по ESC
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                hideBookModal();
            }
        });
    </script>
</body>
</html>'''
        
        return html

# Запускаем сервер
PORT = 8000

print(f"📊 Загружаем статистику...")
stats = get_database_stats()
websites = get_all_websites()
print(f"✅ Найдено: {stats['total_books']} книг, {stats['total_offers']} предложений")
print(f"🏪 Магазины: {', '.join(websites)}")

print(f"\n🌐 Запускаем веб-сервер на порту {PORT}...")
print(f"📚 Откройте в браузере: http://localhost:{PORT}")
print("="*60)
print("✨ ИСПРАВЛЕНИЯ:")
print("   1. Кнопка 'Сбросить всё' теперь работает (ссылка на главную)")
print("   2. Убраны все упоминания ISBN")
print("   3. Поисковая строка по центру")
print("   4. Убран смайлик из названия сайта")
print("   5. Поиск теперь работает вместе с фильтрами")
print("="*60)
print("🛑 Для остановки нажмите Ctrl+C")
print("="*60)

try:
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    with socketserver.TCPServer(("", PORT), BookWebsiteHandler) as httpd:
        print(f"✅ Сервер запущен успешно!")
        print(f"📍 Адрес: http://localhost:{PORT}")
        print("="*60)
        httpd.serve_forever()
        
except OSError as e:
    if "10048" in str(e):
        print(f"❌ Порт {PORT} уже занят!")
        print("🔧 Решение: Запустите с другим портом (например, 8080)")
        input("Нажмите Enter для выхода...")
    else:
        print(f"❌ Ошибка: {e}")
        input("Нажмите Enter для выхода...")
except KeyboardInterrupt:
    print("\n🛑 Сервер остановлен пользователем")
except Exception as e:
    print(f"❌ Неизвестная ошибка: {e}")
    input("Нажмите Enter для выхода...")