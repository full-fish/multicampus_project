import json
import time
import random
import os
import gc
from datetime import datetime

import undetected_chromedriver as uc
from selenium.common.exceptions import (
    UnexpectedAlertPresentException,
    NoSuchWindowException,
    WebDriverException,
)

from get_product_urls import get_product_urls, get_category_product_urls
from get_product_reviews import get_product_reviews


# =========================
# 로그 함수
# =========================
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =========================
# 메인 로직
# =========================
def main():
    MODE = "CATEGORY"
    TARGETS = {"메이크업픽서": "486574"}

    PRODUCT_LIMIT = 200
    REVIEW_TARGET = 200
    MAX_REVIEWS_PER_SEARCH = 50000

    log("전체 크롤링 작업 시작")

    try:
        iterable = enumerate(TARGETS.items())

        for k_idx, item in iterable:
            search_start_time = time.time()

            search_key = item[0]
            search_id = item[1]

            crawled_data_list = []
            keyword_total_collected = 0
            keyword_total_text = 0
            total_rating_distribution = {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}
            processed_urls = set()

            # =========================
            # 이어하기 기능
            # =========================
            resume_filenames = [
                f"result_{search_key}_interrupted.json",
                f"result_{search_key}.json",
            ]

            for fname in resume_filenames:
                if os.path.exists(fname):
                    try:
                        with open(fname, "r", encoding="utf-8") as f:
                            existing_data = json.load(f)

                        crawled_data_list = existing_data.get("data", [])
                        for row in crawled_data_list:
                            p_url = row.get("product_info", {}).get("product_url")
                            if p_url:
                                processed_urls.add(p_url)

                        keyword_total_collected = existing_data.get(
                            "total_collected_reviews", 0
                        )
                        keyword_total_text = existing_data.get(
                            "total_text_reviews", 0
                        )
                        total_rating_distribution = existing_data.get(
                            "total_rating_distribution",
                            total_rating_distribution,
                        )

                        log(
                            f"[이어하기] {fname} 로드 완료 "
                            f"(상품 {len(crawled_data_list)}개, 리뷰 {keyword_total_collected}개)"
                        )
                        break
                    except Exception as e:
                        log(f"[이어하기 실패] {e}")

            # =========================
            # URL 수집
            # =========================
            log(f"[{search_key}] URL 수집 시작")

            URL_COLLECT_MAX_RETRIES = 3
            urls = []

            for attempt in range(URL_COLLECT_MAX_RETRIES):
                log(f"URL 수집 시도 {attempt+1}/{URL_COLLECT_MAX_RETRIES}")

                options = uc.ChromeOptions()
                options.add_argument("--no-first-run")
                options.add_argument("--no-service-autorun")
                options.add_argument("--password-store=basic")
                options.add_argument("--window-size=1920,1080")
                options.add_argument("--blink-settings=imagesEnabled=false")

                driver = uc.Chrome(options=options, use_subprocess=False)

                try:
                    urls = get_category_product_urls(
                        driver, search_id, max_products=PRODUCT_LIMIT
                    )
                    urls = list(set(urls))

                    if processed_urls:
                        urls = [u for u in urls if u not in processed_urls]

                    log(f"[{search_key}] URL 확보 완료: {len(urls)}개")
                    break

                except Exception as e:
                    log(f"[URL 수집 오류] {e}")
                    urls = []

                finally:
                    driver = driver_cleanup(driver)

                time.sleep(20)

            if not urls and not processed_urls:
                log(f"[{search_key}] URL 수집 실패 → 스킵")
                continue

            # =========================
            # 리뷰 수집
            # =========================
            log(f"[{search_key}] 상세 리뷰 수집 시작")

            driver = None
            driver_collected_count = 0

            for idx, url in enumerate(urls):
                if keyword_total_collected >= MAX_REVIEWS_PER_SEARCH:
                    log(f"[{search_key}] 타겟 리뷰 수 도달 → 종료")
                    break

                print(f"\n   [{idx+1}/{len(urls)}] 상품 처리")

                for attempt in range(3):
                    try:
                        if driver is None:
                            options = uc.ChromeOptions()
                            options.add_argument("--no-first-run")
                            options.add_argument("--no-service-autorun")
                            options.add_argument("--password-store=basic")
                            options.add_argument("--window-size=1920,1080")
                            options.add_argument("--blink-settings=imagesEnabled=false")
                            driver = uc.Chrome(options=options, use_subprocess=False)
                            driver_collected_count = 0

                        data = get_product_reviews(
                            driver,
                            url,
                            idx + 1,
                            target_review_count=REVIEW_TARGET,
                            driver_collected_count=driver_collected_count,
                        )

                        if data and data.get("skip_official_product"):
                            print("     -> 브랜드 공식 상품 스킵")
                            break

                        r_data = data.get("reviews", {})
                        current_collected = r_data.get("total_count", 0)

                        if current_collected == 0:
                            print("     -> 리뷰 0개 스킵")
                            break

                        keyword_total_collected += current_collected
                        keyword_total_text += r_data.get("text_count", 0)
                        driver_collected_count += current_collected

                        if driver_collected_count >= 800:
                            log("드라이버 피로도 누적 → 재시작")
                            driver = driver_cleanup(driver)
                            driver_collected_count = 0

                        product_rating = data.get("product_info", {}).get(
                            "rating_distribution", {}
                        )
                        for score, cnt in product_rating.items():
                            total_rating_distribution[score] += cnt

                        crawled_data_list.append(data)

                        print(
                            f"     -> 성공: {current_collected}개 "
                            f"(누적 {keyword_total_collected})"
                        )
                        break

                    except Exception as e:
                        print(f"     -> 오류: {e}")
                        driver = driver_cleanup(driver)

            if driver:
                driver = driver_cleanup(driver)

            # =========================
            # 저장
            # =========================
            result_json = {
                "search_name": search_key,
                "total_collected_reviews": keyword_total_collected,
                "total_text_reviews": keyword_total_text,
                "total_product": len(crawled_data_list),
                "total_rating_distribution": total_rating_distribution,
                "data": crawled_data_list,
            }

            filename = f"result_{search_key}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(result_json, f, indent=2, ensure_ascii=False)

            log(f"[{search_key}] 저장 완료 → {filename}")

            elapsed = time.time() - search_start_time
            h = int(elapsed // 3600)
            m = int((elapsed % 3600) // 60)
            s = int(elapsed % 60)

            log(f"[{search_key}] 처리 시간 {h}h {m}m {s}s")

    except KeyboardInterrupt:
        log("사용자 중단 발생")


# =========================
# 드라이버 정리
# =========================
def driver_cleanup(driver):
    try:
        driver.quit()
        del driver
        gc.collect()
        time.sleep(22)
    except Exception:
        pass
    return None


if __name__ == "__main__":
    main()
    