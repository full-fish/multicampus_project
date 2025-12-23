import json
from preprocess_format import preprocess_format
from brand_standardizer import brand_standardizer
from drop_missing_val_splitter import drop_missing_val_splitter


def main():
    """
    전처리 파이프라인을 순차적으로 실행합니다.
    1. preprocess_format: 데이터 포맷 변환 (날짜, 정수형 변환, 텍스트 정규화, 중복 제거)
    2. brand_standardizer: 브랜드 및 카테고리 표준화, 상품명 정제
    3. drop_missing_val_splitter: 결측치 제거 및 텍스트 유무에 따라 파일 분할
    """

    # 입력 파일 경로
    INPUT_FILE = "result_오일.json"
    OUTPUT_WITH_TEXT = "with_text_drop_missing.json"
    OUTPUT_WITHOUT_TEXT = "without_text_drop_missing.json"

    print("=" * 60)
    print("전처리 파이프라인 시작")
    print("=" * 60)

    # 1단계: 원본 JSON 파일 로드
    print(f"\n[1단계] JSON 파일 로드 중: {INPUT_FILE}")
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✓ 파일 로드 완료 - 상품 수: {len(data.get('data', []))}개")
    except FileNotFoundError:
        print(f"✗ 에러: {INPUT_FILE} 파일을 찾을 수 없습니다.")
        return
    except json.JSONDecodeError:
        print(f"✗ 에러: {INPUT_FILE}이 올바른 JSON 형식이 아닙니다.")
        return

    # 2단계: 데이터 포맷 전처리
    print("\n[2단계] 데이터 포맷 전처리 중...")
    print("  - 날짜/시간 형식 변환")
    print("  - 정수형 변환 (id, score, price 등)")
    print("  - 텍스트 정규화 (이모지 제거, 자모음 반복 축소)")
    print("  - 중복 리뷰 제거")

    # 임시 파일에 저장하고 preprocess_format 호출
    temp_file = "temp_input.json"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    data = preprocess_format(temp_file)

    # 임시 파일 삭제
    import os

    if os.path.exists(temp_file):
        os.remove(temp_file)

    print(
        f"✓ 포맷 전처리 완료 - 상품 수: {data.get('total_product', 0)}개, 리뷰 수: {data.get('total_collected_reviews', 0)}개"
    )

    # 3단계: 브랜드 표준화
    print("\n[3단계] 브랜드 및 상품명 표준화 중...")
    print("  - 브랜드명 통일")
    print("  - 카테고리 표준화")
    print("  - 상품명 정제 (노이즈 제거)")
    print("  - 상품 토큰 생성")

    data = brand_standardizer(data)
    print("✓ 브랜드 표준화 완료")

    # 4단계: 결측치 제거 및 파일 분할
    print("\n[4단계] 결측치 제거 및 파일 분할 중...")
    print("  - 빈 필드 제거 (helpful_count=0, has_image=False, 빈 문자열)")
    print("  - 텍스트 있는 리뷰 / 없는 리뷰 분리")

    with_text, without_text = drop_missing_val_splitter(data)
    print("✓ 결측치 제거 및 분할 완료")

    # 5단계: 최종 결과 저장
    print("\n[5단계] 결과 파일 저장 중...")

    with open(OUTPUT_WITH_TEXT, "w", encoding="utf-8") as f:
        json.dump(with_text, f, ensure_ascii=False, indent=2)
    print(f"✓ 텍스트 포함 파일 저장 완료: {OUTPUT_WITH_TEXT}")
    print(f"  - 상품 수: {with_text['total_product']}개")
    print(f"  - 리뷰 수: {with_text['total_collected_reviews']}개")

    with open(OUTPUT_WITHOUT_TEXT, "w", encoding="utf-8") as f:
        json.dump(without_text, f, ensure_ascii=False, indent=2)
    print(f"✓ 텍스트 미포함 파일 저장 완료: {OUTPUT_WITHOUT_TEXT}")
    print(f"  - 상품 수: {without_text['total_product']}개")
    print(f"  - 리뷰 수: {without_text['total_collected_reviews']}개")

    print("\n" + "=" * 60)
    print("전처리 파이프라인 완료!")
    print("=" * 60)


if __name__ == "__main__":
    main()
