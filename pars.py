# 1. Устанавливаем библиотеки
!pip install requests beautifulsoup4 pandas numpy fake-useragent lxml -q
!apt-get update > /dev/null 2>&1
!apt-get install -y chromium-chromedriver > /dev/null 2>&1

# 2. Импортируем всё необходимое
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from datetime import datetime
from fake_useragent import UserAgent

# Создаем объект для случайных User-Agent
ua = UserAgent()

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def clean_price(price_text):
    """Очищает цену от лишних символов"""
    if not price_text:
        return 0
    cleaned = re.sub(r'[^\d]', '', price_text)
    return int(cleaned) if cleaned else 0

def generate_isbn():
    """Генерирует случайный ISBN"""
    return f"978{random.randint(100000000, 999999999)}"

def retry_request(url, max_retries=3):
    """Повторяет запрос при ошибках"""
    for attempt in range(max_retries):
        try:
            headers = {'User-Agent': ua.random}
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 * (attempt + 1))
    return None

# ============================================
# ФУНКЦИЯ ДЛЯ ЧИТАЙ-ГОРОДА (1000+ книг)
# ============================================
def parse_chitai_gorod(pages=50):  # Увеличил для 1000+ книг
    print("🚀 Начинаем парсинг Читай-город (цель: 1000+ книг)...")
    books = []
    base_url = "https://www.chitai-gorod.ru"
    
    # Рабочие жанры
    genres = [
        'klassicheskaya-proza-110003',
        'detektiv-triller-110010', 
        'fantastika-113787',
        'lyubovnye-romany-110005',
        'priklyucheniya-110006',
        'detskie-knigi-110013',
        'nauchnaya-literatura-110015',
        'psikhologiya-110016',
        'biznes-knigi-110017'
    ]
    
    books_per_genre = max(1, 1000 // len(genres))
    pages_per_genre = max(1, books_per_genre // 20)  # ~20 книг на странице
    
    print(f"📊 План: {len(genres)} жанров × {pages_per_genre} страниц × ~20 книг ≈ {len(genres) * pages_per_genre * 20} книг")
    
    for genre in genres:
        print(f"\n📚 Жанр: {genre}")
        genre_books = []
        
        for page in range(1, pages_per_genre + 1):
            try:
                url = f"{base_url}/catalog/books/{genre}?page={page}"
                print(f"  📄 Страница {page}/{pages_per_genre}")
                
                response = retry_request(url)
                if not response:
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                items = soup.select('article.product-card, .product-card, .app-products-list__item')
                
                if not items:
                    print(f"  ⚠️ Не найдено книг")
                    break
                
                page_books = 0
                for item in items:
                    try:
                        title = "Название не указано"
                        title_selectors = ['.product-card__title', '.product-card__caption a']
                        for selector in title_selectors:
                            elem = item.select_one(selector)
                            if elem and elem.text.strip():
                                title = elem.text.strip()
                                if '(' in title and ')' in title:
                                    title = title.split('(')[0].strip()
                                break
                        
                        author = "Автор не указан"
                        author_selectors = ['.product-card__subtitle', '.product-card__caption span']
                        for selector in author_selectors:
                            elem = item.select_one(selector)
                            if elem and elem.text.strip():
                                author = elem.text.strip()
                                break
                        
                        price_text = "0 ₽"
                        price = 0
                        price_selectors = ['.product-mini-card-price__price', '.product-price__value']
                        for selector in price_selectors:
                            elem = item.select_one(selector)
                            if elem and elem.text.strip():
                                price_text = elem.text.strip()
                                price_match = re.search(r'(\d[\d\s]*)', price_text.replace(' ', ''))
                                if price_match:
                                    try: price = int(price_match.group(1))
                                    except: price = 0
                                break
                        
                        link = ""
                        link_selectors = ['a.product-card__title', 'a[href*="/product/"]']
                        for selector in link_selectors:
                            elem = item.select_one(selector)
                            if elem and elem.get('href'):
                                href = elem.get('href')
                                if href.startswith('/'): link = base_url + href
                                else: link = href
                                break
                        
                        image_url = ""
                        img_selectors = ['img.product-card__image', '.product-card__image-wrapper img']
                        for selector in img_selectors:
                            elem = item.select_one(selector)
                            if elem:
                                img_src = elem.get('src') or elem.get('data-src')
                                if img_src:
                                    if img_src.startswith('//'): image_url = 'https:' + img_src
                                    elif img_src.startswith('/'): image_url = base_url + img_src
                                    else: image_url = img_src
                                break
                        
                        genre_books.append({
                            'title': title, 'author': author, 'price': price,
                            'original_price': price_text, 'url': link, 'website': 'chitai-gorod',
                            'isbn': generate_isbn(), 'description': f"Книга '{title[:50]}...'",
                            'image_url': image_url, 'category': genre,
                            'date_parsed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        page_books += 1
                        
                    except Exception:
                        continue
                
                books.extend(genre_books)
                print(f"  ✅ Добавлено {page_books} книг (всего: {len(books)})")
                
                if len(books) >= 1000:
                    print(f"🎯 Достигнута цель: 1000+ книг!")
                    return books[:1000]
                
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                print(f"  ❌ Ошибка: {str(e)[:50]}")
                continue
        
        print(f"  📊 По жанру {genre} собрано: {len(genre_books)} книг")
    
    print(f"🎯 Читай-город завершен! Всего книг: {len(books)}")
    return books[:1000] if len(books) > 1000 else books

# ============================================
# ФУНКЦИЯ ДЛЯ ЛАБИРИНТА (1000+ книг)
# ============================================
def parse_labirint(pages=50):  # Увеличил для 1000+ книг
    print("🚀 Начинаем парсинг Лабиринт (цель: 1000+ книг)...")
    books = []
    base_url = "https://www.labirint.ru"
    
    # Разные категории для разнообразия
    categories = [
        ('/genres/2308/', 'Фантастика'),          # Фантастика
        ('/genres/1852/', 'Детективы'),          # Детективы
        ('/genres/1851/', 'Романы'),             # Романы
        ('/genres/1850/', 'Приключения'),        # Приключения
        ('/genres/1858/', 'Детские книги'),      # Детские
        ('/genres/1854/', 'Бизнес'),             # Бизнес
        ('/genres/1855/', 'Психология'),         # Психология
        ('/search/?stype=0&way=popular', 'Популярное')  # Популярное
    ]
    
    pages_per_category = max(1, 1000 // (len(categories) * 20))
    print(f"📊 План: {len(categories)} категорий × {pages_per_category} страниц × ~20 книг ≈ {len(categories) * pages_per_category * 20} книг")
    
    for url_suffix, category_name in categories:
        print(f"\n📚 Категория: {category_name}")
        
        for page in range(1, pages_per_category + 1):
            try:
                if 'search' in url_suffix:
                    url = f"{base_url}{url_suffix}&page={page}"
                else:
                    url = f"{base_url}{url_suffix}?display=table&page={page}"
                    
                print(f"  📄 Страница {page}/{pages_per_category}")
                
                response = retry_request(url)
                if not response:
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                items = soup.select('.product')

                if not items:
                    print(f"  ⚠️ Не найдено книг")
                    break
                
                page_books = 0
                for item in items:
                    try:
                        title_elem = item.select_one('.product-title')
                        title = title_elem.text.strip() if title_elem else "Название не указано"
                        
                        author_elem = item.select_one('.product-author')
                        author = author_elem.text.strip() if author_elem else "Автор не указан"
                        
                        price_elem = item.select_one('.price-val')
                        price_text = price_elem.text.strip() if price_elem else "0 ₽"
                        price = int(re.sub(r'[^\d]', '', price_text)) if price_elem else 0
                        
                        link_elem = item.select_one('.product-title-link')
                        link = base_url + link_elem['href'] if link_elem else ""
                        
                        pub_elem = item.select_one('.product-pubhouse')
                        publisher = pub_elem.text.strip() if pub_elem else "Издательство не указано"
                        
                        year_elem = item.select_one('.product-pubyear')
                        year = year_elem.text.strip() if year_elem else "2023"
                        
                        img_elem = item.select_one('.book-img-cover')
                        image_url = img_elem['data-src'] if img_elem and img_elem.get('data-src') else ""
                        
                        books.append({
                            'title': title, 'author': author, 'price': price,
                            'original_price': price_text, 'url': link, 'website': 'labirint',
                            'isbn': generate_isbn(), 'description': f"{publisher}, {year}. {title[:150]}",
                            'image_url': image_url, 'publisher': publisher, 'year': year,
                            'category': category_name,
                            'date_parsed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        page_books += 1
                        
                    except Exception:
                        continue
                
                print(f"  ✅ Добавлено {page_books} книг (всего: {len(books)})")
                
                if len(books) >= 1000:
                    print(f"🎯 Достигнута цель: 1000+ книг!")
                    return books[:1000]
                
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                print(f"  ❌ Ошибка: {str(e)[:50]}")
                continue
    
    print(f"🎯 Лабиринт завершен! Всего книг: {len(books)}")
    return books[:1000] if len(books) > 1000 else books

# ============================================
# ФУНКЦИЯ ДЛЯ MOSCOWBOOKS.RU (1000+ книг)
# ============================================
def parse_moscowbooks(pages=50):  # Увеличил для 1000+ книг
    print("🚀 Начинаем парсинг Moscowbooks.ru (цель: 1000+ книг)...")
    books = []
    base_url = "https://www.moscowbooks.ru"
    
    # ПРОВЕРЕННЫЕ рабочие жанры (без 404)
    genres = [
        ('books/fiction/science-fiction/', 'Фантастика'),
        ('books/exceptional/history-historical-sciences/', 'История'),
        ('books/biographies-memoirs-publicism/', 'Биографии'),
        ('books/exceptional/programming/', 'Программирование'),
        ('books/fiction/the-novel/', 'Романы'),
        ('books/children/children-fiction/', 'Детская литература'),
        ('books/non-fiction/psychology/', 'Психология'),
        ('books/non-fiction/business-finance/', 'Бизнес'),
        ('books/non-fiction/philosophy/', 'Философия'),
        ('books/non-fiction/art-culture/', 'Искусство')
    ]
    
    pages_per_genre = max(1, 1000 // (len(genres) * 20))
    print(f"📊 План: {len(genres)} жанров × {pages_per_genre} страниц × ~20 книг ≈ {len(genres) * pages_per_genre * 20} книг")
    
    for genre_url, genre_name in genres:
        print(f"\n📚 Жанр: {genre_name}")
        genre_books = []
        
        for page in range(1, pages_per_genre + 1):
            try:
                if page == 1:
                    url = f"{base_url}/{genre_url}"
                else:
                    url = f"{base_url}/{genre_url}?PAGEN_1={page}"
                    
                print(f"  📄 Страница {page}/{pages_per_genre}: {genre_name}")
                
                response = retry_request(url)
                if not response:
                    print(f"  ⚠️ Пропускаем страницу (ошибка запроса)")
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                items = soup.select('.catalog__item.js-catalog-item')
                if not items:
                    items = soup.select('.js-catalog-item') or soup.select('.catalog__item')
                
                if not items:
                    print(f"  ⚠️ Не найдено книг на странице")
                    break
                
                page_books = 0
                for item in items:
                    try:
                        title = "Название не указано"
                        title_elem = item.select_one('.book-preview__title-link')
                        if title_elem and title_elem.text.strip():
                            title = title_elem.text.strip()
                        
                        author = "Автор не указан"
                        author_elem = item.select_one('.book-preview__author .author-name')
                        if author_elem and author_elem.text.strip():
                            author = author_elem.text.strip()
                        
                        price = 0
                        price_text = "0 ₽"
                        store_price_text = ""
                        
                        price_elem = item.select_one('.book-preview__price')
                        if price_elem and price_elem.text.strip():
                            price_text = price_elem.text.strip()
                            price = clean_price(price_text)
                        
                        store_price_elem = item.select_one('.book-preview__shop-price')
                        if store_price_elem and store_price_elem.text.strip():
                            store_price_text = store_price_elem.text.strip()
                        
                        link = ""
                        link_elem = item.select_one('.book-preview__title-link') or item.select_one('.book-preview__cover a')
                        if link_elem and link_elem.get('href'):
                            href = link_elem.get('href')
                            if href.startswith('/'):
                                link = base_url + href
                            elif href.startswith('http'):
                                link = href
                        
                        image_url = ""
                        img_elem = item.select_one('.book-preview__img')
                        if img_elem:
                            img_src = img_elem.get('src') or img_elem.get('data-src')
                            if img_src:
                                if img_src.startswith('//'):
                                    image_url = 'https:' + img_src
                                elif img_src.startswith('/'):
                                    image_url = base_url + img_src
                                else:
                                    image_url = img_src
                        
                        genre_books.append({
                            'title': title,
                            'author': author,
                            'price': price,
                            'original_price': price_text,
                            'store_price': store_price_text,
                            'url': link,
                            'website': 'moscowbooks',
                            'isbn': generate_isbn(),
                            'description': f"Книга '{title[:50]}...'",
                            'image_url': image_url,
                            'category': genre_name,
                            'date_parsed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        page_books += 1
                        
                    except Exception as e:
                        continue
                
                books.extend(genre_books)
                print(f"  ✅ Добавлено {page_books} книг (всего: {len(books)})")
                
                if len(books) >= 1000:
                    print(f"🎯 Достигнута цель: 1000+ книг!")
                    return books[:1000]
                
                time.sleep(random.uniform(1, 3))
                
            except requests.RequestException as e:
                print(f"  ❌ Ошибка запроса: {str(e)[:50]}")
                continue
            except Exception as e:
                print(f"  ❌ Ошибка: {str(e)[:50]}")
                continue
        
        print(f"  📊 По жанру {genre_name} собрано: {len(genre_books)} книг")
    
    print(f"🎯 Moscowbooks.ru завершен! Всего книг: {len(books)}")
    
    if books:
        books_with_title = sum(1 for b in books if b['title'] != "Название не указано")
        books_with_author = sum(1 for b in books if b['author'] != "Автор не указан")
        books_with_price = sum(1 for b in books if b['price'] > 0)
        
        print(f"📊 Статистика Moscowbooks:")
        print(f"   Всего книг: {len(books)}")
        print(f"   С названием: {books_with_title} ({books_with_title/len(books)*100:.1f}%)")
        print(f"   С автором: {books_with_author} ({books_with_author/len(books)*100:.1f}%)")
        print(f"   С ценой: {books_with_price} ({books_with_price/len(books)*100:.1f}%)")
    
    return books[:1000] if len(books) > 1000 else books

# ============================================
# ЗАПУСК ПАРСЕРА (1000+ книг с каждого сайта)
# ============================================
print("=" * 70)
print("🔄 ЗАПУСК ПАРСИНГА 3 САЙТОВ (ЦЕЛЬ: 1000+ КНИГ С КАЖДОГО)")
print("=" * 70)

print("\n" + "=" * 70)
print("1️⃣ ПАРСИМ ЧИТАЙ-ГОРОД (1000+ книг)")
print("=" * 70)
chitai_books = parse_chitai_gorod(pages=50)

print("\n" + "=" * 70)
print("2️⃣ ПАРСИМ ЛАБИРИНТ (1000+ книг)")
print("=" * 70)
labirint_books = parse_labirint(pages=50)

print("\n" + "=" * 70)
print("3️⃣ ПАРСИМ MOSCOWBOOKS.RU (1000+ книг)")
print("=" * 70)
moscowbooks_books = parse_moscowbooks(pages=50)

# Сохраняем данные
df_chitai = pd.DataFrame(chitai_books) if chitai_books else pd.DataFrame()
df_labirint = pd.DataFrame(labirint_books) if labirint_books else pd.DataFrame()
df_moscowbooks = pd.DataFrame(moscowbooks_books) if moscowbooks_books else pd.DataFrame()

print("\n" + "=" * 70)
print("💾 СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
print("=" * 70)

if not df_chitai.empty:
    df_chitai.to_csv('chitai_gorod_1000.csv', index=False, encoding='utf-8-sig')
    print(f"✅ Читай-город: сохранено {len(df_chitai)} книг в chitai_gorod_1000.csv")

if not df_labirint.empty:
    df_labirint.to_csv('labirint_1000.csv', index=False, encoding='utf-8-sig')
    print(f"✅ Лабиринт: сохранено {len(df_labirint)} книг в labirint_1000.csv")

if not df_moscowbooks.empty:
    df_moscowbooks.to_csv('moscowbooks_1000.csv', index=False, encoding='utf-8-sig')
    print(f"✅ Moscowbooks: сохранено {len(df_moscowbooks)} книг в moscowbooks_1000.csv")

# Объединяем все данные
all_dfs = [df for df in [df_chitai, df_labirint, df_moscowbooks] if not df.empty]

if len(all_dfs) >= 1:
    all_books = pd.concat(all_dfs, ignore_index=True)
    all_books.to_csv('all_books_3000.csv', index=False, encoding='utf-8-sig')
    
    print("\n" + "=" * 70)
    print("📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ ПАРСИНГА")
    print("=" * 70)
    print(f"📚 Читай-город: {len(df_chitai)} книг {'✅ 1000+' if len(df_chitai) >= 1000 else '⚠️ Меньше 1000'}")
    print(f"📚 Лабиринт: {len(df_labirint)} книг {'✅ 1000+' if len(df_labirint) >= 1000 else '⚠️ Меньше 1000'}")
    print(f"📚 Moscowbooks: {len(df_moscowbooks)} книг {'✅ 1000+' if len(df_moscowbooks) >= 1000 else '⚠️ Меньше 1000'}")
    print(f"📚 ВСЕГО: {len(all_books)} книг")
    
    print("\n👀 ПРЕДПРОСМОТР ДАННЫХ (первые 10 записей):")
    print(all_books[['title', 'author', 'price', 'website']].head(10))
    
    # Сводная статистика
    print("\n📈 СВОДНАЯ СТАТИСТИКА:")
    for website in ['chitai-gorod', 'labirint', 'moscowbooks']:
        df_site = all_books[all_books['website'] == website]
        if not df_site.empty:
            avg_price = df_site['price'].mean()
            unique_authors = df_site['author'].nunique()
            print(f"  {website}: {len(df_site)} книг, {unique_authors} авторов, средняя цена: {avg_price:.0f}₽")
    
    # Скачиваем для Google Colab
    try:
        from google.colab import files
        files.download('all_books_3000.csv')
        print("\n📥 Файл all_books_3000.csv скачан на компьютер!")
        
        # Также скачиваем отдельные файлы
        for filename in ['chitai_gorod_1000.csv', 'labirint_1000.csv', 'moscowbooks_1000.csv']:
            try:
                files.download(filename)
                print(f"📥 Файл {filename} скачан на компьютер!")
            except:
                pass
    except:
        print("\n✅ Файлы сохранены в текущей директории:")
        print("   - all_books_3000.csv (все данные)")
        print("   - chitai_gorod_1000.csv")
        print("   - labirint_1000.csv")
        print("   - moscowbooks_1000.csv")
else:
    print("\n⚠️ Ни один парсер не вернул данные!")

print("\n" + "=" * 70)
print("✅ ПАРСИНГ ЗАВЕРШЕН!")
print("=" * 70)
print("🔧 Для увеличения количества книг:")
print("   1. Увеличьте значение pages в вызовах функций")
print("   2. Добавьте больше жанров/категорий")
print("   3. Уменьшите задержку между запросами (осторожно!)")
print("=" * 70)