import json
import time
import random
import undetected_chromedriver as uc
from get_product_urls import get_product_urls
from get_product_reviews import get_product_reviews


def main():
    KEYWORDS = ["사과", "배", "포도"]
    PRODUCT_LIMIT = 3
    REVIEW_TARGET = 30

    print(">>> 브라우저를 실행합니다...")
    options = uc.ChromeOptions()
    options.add_argument("--no-first-run")
    options.add_argument("--no-service-autorun")
    options.add_argument("--password-store=basic")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--blink-settings=imagesEnabled=false")

    driver = uc.Chrome(options=options, use_subprocess=False)

    try:
        for k_idx, keyword in enumerate(KEYWORDS):
            try:
                print(f"\n{'='*50}")
                print(f">>> [{k_idx+1}/{len(KEYWORDS)}] 키워드 검색 시작: {keyword}")
                print(f"{'='*50}")

                # 1. URL 수집
                urls = get_product_urls(driver, keyword, max_products=PRODUCT_LIMIT)
                print(f">>> [{keyword}] 수집된 URL: {len(urls)}개")

                crawled_data_list = []
                top_category = ""

                # 2. 리뷰 수집
                for idx, url in enumerate(urls):
                    print(f"\n   [{idx+1}/{len(urls)}] 상품 크롤링 중 ({keyword})...")

                    try:
                        data = get_product_reviews(
                            driver, url, idx + 1, target_review_count=REVIEW_TARGET
                        )

                        if data["product_info"]:
                            current_category = data["product_info"].get("category_path")
                            if not top_category and current_category:
                                top_category = current_category

                            crawled_data_list.append(data)
                            print(f"     -> 완료: 리뷰 {data['reviews']['count']}개")
                        else:
                            print("     -> 실패")

                        time.sleep(random.uniform(3, 5))

                    except Exception as e:
                        print(f"     -> 에러: {e}")

                # 3. JSON 구조 생성 (해당 키워드용)
                result_json = {
                    "search_name": keyword,
                    "category": top_category,
                    "data": crawled_data_list,
                }

                # 4. JSON 파일 저장 (파일명에 키워드 포함)
                if crawled_data_list:
                    filename = f"result_{keyword}.json"
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(result_json, f, indent=2, ensure_ascii=False)
                    print(f"\n✅ [{keyword}] 저장 완료: {filename}")
                else:
                    print(f"\n[{keyword}] 수집된 데이터가 없습니다.")

                # 다음 키워드로 넘어가기 전 잠시 대기 (차단 방지)
                time.sleep(random.uniform(5, 8))

            except Exception as e:
                print(f"\n!!! [{keyword}] 처리 중 치명적 오류 발생: {e}")
                continue  # 에러가 나도 다음 키워드로 계속 진행

    finally:
        print("\n>>> 모든 작업이 끝났으므로 브라우저를 종료합니다.")
        driver.quit()


if __name__ == "__main__":
    main()
