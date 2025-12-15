from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import random
import urllib.parse


def get_product_urls(driver, keyword, max_products=5):
    encoded_keyword = urllib.parse.quote(keyword)
    search_url = (
        f"https://www.coupang.com/np/search?component=&q={encoded_keyword}&channel=user"
    )

    product_urls = []

    try:
        print(f"[get_urls] '{keyword}' 검색 페이지 접속 중...")
        driver.get(search_url)
        time.sleep(random.uniform(2, 4))

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "product-list"))
        )

        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(2)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        links = soup.select("ul#product-list > li > a")

        print(f"[get_urls] 발견된 상품 링크: {len(links)}개")

        count = 0
        for link in links:
            if count >= max_products:
                break

            href = link.get("href")

            if not href or "javascript" in href or href == "#":
                continue

            if href.startswith("/"):
                full_url = f"https://www.coupang.com{href}"
            else:
                full_url = href

            product_urls.append(full_url)
            count += 1

    except Exception as e:
        print(f"[get_urls] 에러 발생: {e}")

    return product_urls
