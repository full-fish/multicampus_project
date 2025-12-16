import json
import time
import random
import undetected_chromedriver as uc
from selenium.common.exceptions import (
    UnexpectedAlertPresentException,
    NoSuchWindowException,
    WebDriverException,
)

# 모듈 이름은 사용자의 환경에 맞게 유지
from get_product_urls import get_product_urls
from get_product_reviews import get_product_reviews


def main():
    start_time = time.time()
    KEYWORDS = ["사과"]
    PRODUCT_LIMIT = 10  # 키워드 당 수집할 상품 수
    REVIEW_TARGET = 10000  # 목표 리뷰 수

    print(">>> 전체 작업을 시작합니다...")

    try:
        for k_idx, keyword in enumerate(KEYWORDS):
            crawled_data_list = []
            top_category = ""
            keyword_total_collected = 0
            keyword_total_text = 0

            # ---------------------------------------------------------
            # [단계 1] URL 수집
            # ---------------------------------------------------------
            print(f"\n{'='*50}")
            print(f">>> [{k_idx+1}/{len(KEYWORDS)}] '{keyword}' URL 수집 시작")
            print(f"{'='*50}")

            # URL 수집용 옵션 생성
            options = uc.ChromeOptions()
            options.add_argument("--no-first-run")
            options.add_argument("--no-service-autorun")
            options.add_argument("--password-store=basic")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--blink-settings=imagesEnabled=false")

            driver = uc.Chrome(options=options, use_subprocess=False)
            try:
                urls = get_product_urls(driver, keyword, max_products=PRODUCT_LIMIT)
                print(f">>> [{keyword}] URL {len(urls)}개 확보 완료")
            except Exception as e:
                print(f">>> URL 수집 중 에러: {e}")
                urls = []
            finally:
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(5)  # 브라우저 종료 후 잠시 대기

            if not urls:
                print(f">>> [{keyword}] 수집된 URL이 없어 넘어갑니다.")
                continue

            # ---------------------------------------------------------
            # [단계 2] 개별 상품 리뷰 수집
            # ---------------------------------------------------------
            print(f">>> [{keyword}] 상세 리뷰 수집 시작 (상품마다 브라우저 재실행)")

            for idx, url in enumerate(urls):
                print(f"\n   [{idx+1}/{len(urls)}] 상품 접속 준비 중... ({keyword})")

                options = uc.ChromeOptions()
                options.add_argument("--no-first-run")
                options.add_argument("--no-service-autorun")
                options.add_argument("--password-store=basic")
                options.add_argument("--window-size=1920,1080")
                options.add_argument("--blink-settings=imagesEnabled=false")

                try:
                    driver = uc.Chrome(options=options, use_subprocess=False)
                    time.sleep(1)

                except WebDriverException as e:
                    print(f"     -> [치명적 에러] 드라이버 실행 실패: {e}")
                    time.sleep(10)
                    continue

                try:
                    # 데이터 수집 함수 호출
                    data = get_product_reviews(
                        driver, url, idx + 1, target_review_count=REVIEW_TARGET
                    )

                    if data and data.get("product_info"):
                        # 카테고리 정보 업데이트
                        current_category = data["product_info"].get("category_path")
                        if not top_category and current_category:
                            top_category = current_category

                        r_data = data.get("reviews", {})
                        keyword_total_collected += r_data.get("total_count", 0)
                        keyword_total_text += r_data.get("text_count", 0)

                        crawled_data_list.append(data)
                        print(
                            f"     -> [완료] 전체: {r_data.get('total_count')}개 / 글있음: {r_data.get('text_count')}개"
                        )
                    else:
                        print(
                            "     -> [실패] 데이터 없음 (함수 반환값 None 또는 비어있음)"
                        )

                except UnexpectedAlertPresentException:
                    print(
                        "     -> [차단/경고] 페이지 접속 중 알림창(Alert)이 발생했습니다. (서버 오류 등)"
                    )
                except NoSuchWindowException:
                    print(
                        "     -> [브라우저 에러] 브라우저 창이 강제로 종료되었습니다."
                    )
                except Exception as e:
                    print(f"     -> [일반 에러] {e}")

                finally:
                    # 안전하게 종료
                    try:
                        driver.quit()
                    except:
                        pass
                    print("     -> 브라우저 세션 종료")

                # 대기 시간 증가 (안정성 확보를 위해 10~15초 권장)
                sleep_time = random.uniform(10, 15)
                print(f"     -> 다음 상품 대기 중... ({sleep_time:.1f}초)")
                time.sleep(sleep_time)

            # ---------------------------------------------------------
            # [단계 3] 키워드 완료 후 저장
            # ---------------------------------------------------------
            result_json = {
                "search_name": keyword,
                "category": top_category,
                "total_collected_reviews": keyword_total_collected,
                "total_text_reviews": keyword_total_text,
                "data": crawled_data_list,
            }

            if crawled_data_list:
                filename = f"result_{keyword}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(result_json, f, indent=2, ensure_ascii=False)
                print(f"\n [{keyword}] 저장 완료: {filename}")
            else:
                print(f"\n[{keyword}] 수집된 데이터가 없습니다.")

            long_sleep = random.uniform(10, 15)
            print(f">>> 다음 키워드 준비 중 ({long_sleep:.1f}초)...")
            time.sleep(long_sleep)

    except KeyboardInterrupt:
        print("\n>>> 사용자에 의해 작업이 중단되었습니다.")

    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)
        print(f"\n총 실행 시간: {hours}시간 {minutes}분 {seconds}초")


if __name__ == "__main__":
    main()
