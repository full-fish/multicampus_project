import json
import os
import glob
from pathlib import Path
from preprocess_format import preprocess_format
from brand_standardizer import brand_standardizer
from drop_missing_val_splitter import drop_missing_val_splitter
from reviews_with_word2vec import reviews_with_word2vec


def process_single_file(input_path, output_dir):
    """
    단일 파일을 전처리하고 결과를 저장합니다.

    Args:
        input_path: 입력 JSON 파일 경로
        output_dir: 출력 디렉토리 경로
    """
    file_name = os.path.basename(input_path)
    print("\n" + "=" * 60)
    print(f"파일 처리 중: {file_name}")
    print("=" * 60)

    print("\n" + "=" * 60)
    print(f"파일 처리 중: {file_name}")
    print("=" * 60)

    # 1단계: JSON 파일 로드
    print(f"\n[1단계] JSON 파일 로드 중: {file_name}")
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✓ 파일 로드 완료 - 상품 수: {len(data.get('data', []))}개")
    except FileNotFoundError:
        print(f"✗ 에러: {input_path} 파일을 찾을 수 없습니다.")
        return
    except json.JSONDecodeError:
        print(f"✗ 에러: {input_path}이 올바른 JSON 형식이 아닙니다.")
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

    # 5단계: Word2Vec 및 감성 라벨링 (텍스트 있는 데이터만)
    print("\n[5단계] Word2Vec 및 감성 라벨링 처리 중...")
    print("  - 형태소 분석 및 토큰화")
    print("  - 감성 라벨링 (긍정/부정)")
    print("  - Word2Vec 벡터 생성")

    with_text = reviews_with_word2vec(with_text)
    print("✓ Word2Vec 및 감성 라벨링 완료")

    # 6단계: 최종 결과 저장
    print("\n[6단계] 결과 파일 저장 중...")

    # 출력 디렉토리 생성
    os.makedirs(output_dir, exist_ok=True)

    # 파일명 변환: result_오일.json -> processed_오일_with_text.json
    base_name = os.path.splitext(file_name)[0]  # result_오일
    if base_name.startswith("result_"):
        base_name = base_name[7:]  # "result_" 제거 -> 오일

    output_with_text = os.path.join(output_dir, f"processed_{base_name}_with_text.json")
    output_without_text = os.path.join(
        output_dir, f"processed_{base_name}_without_text.json"
    )

    with open(output_with_text, "w", encoding="utf-8") as f:
        json.dump(with_text, f, ensure_ascii=False, indent=2)
    print(f"✓ 텍스트 포함 파일 저장 완료: {output_with_text}")
    print(f"  - 상품 수: {with_text['total_product']}개")
    print(f"  - 리뷰 수: {with_text['total_collected_reviews']}개")

    with open(output_without_text, "w", encoding="utf-8") as f:
        json.dump(without_text, f, ensure_ascii=False, indent=2)
    print(f"✓ 텍스트 미포함 파일 저장 완료: {output_without_text}")
    print(f"  - 상품 수: {without_text['total_product']}개")
    print(f"  - 리뷰 수: {without_text['total_collected_reviews']}개")


def main():
    """
    ./data/pre_data 내 모든 JSON 파일을 순회하며 전처리 파이프라인을 실행합니다.
    결과는 ./data/processed_data에 동일한 폴더 구조로 저장됩니다.
    """
    # 기본 경로 설정
    PRE_DATA_DIR = "./data/pre_data"
    PROCESSED_DATA_DIR = "./data/processed_data"

    print("=" * 60)
    print("전체 전처리 파이프라인 시작")
    print("=" * 60)

    # pre_data 디렉토리의 모든 JSON 파일 찾기
    json_files = glob.glob(os.path.join(PRE_DATA_DIR, "**", "*.json"), recursive=True)

    if not json_files:
        print(f"\n✗ {PRE_DATA_DIR} 디렉토리에서 JSON 파일을 찾을 수 없습니다.")
        return

    print(f"\n총 {len(json_files)}개 파일 발견")

    skipped_count = 0
    processed_count = 0

    for idx, input_path in enumerate(json_files, 1):
        print(f"\n\n{'='*60}")
        print(f"[{idx}/{len(json_files)}] 처리 중")
        print(f"{'='*60}")

        # 상대 경로 계산 (pre_data 기준)
        rel_path = os.path.relpath(input_path, PRE_DATA_DIR)
        rel_dir = os.path.dirname(rel_path)

        # 출력 디렉토리 경로 생성
        output_dir = os.path.join(PROCESSED_DATA_DIR, rel_dir)

        # 출력 파일명 계산
        file_name = os.path.basename(input_path)
        base_name = os.path.splitext(file_name)[0]
        if base_name.startswith("result_"):
            base_name = base_name[7:]

        output_with_text = os.path.join(
            output_dir, f"processed_{base_name}_with_text.json"
        )
        output_without_text = os.path.join(
            output_dir, f"processed_{base_name}_without_text.json"
        )

        # 이미 처리된 파일인지 확인
        if os.path.exists(output_with_text) and os.path.exists(output_without_text):
            print(f"⏭️  이미 처리된 파일입니다. 건너뜁니다.")
            print(f"   - {output_with_text}")
            print(f"   - {output_without_text}")
            skipped_count += 1
            continue

        # 파일 처리
        try:
            process_single_file(input_path, output_dir)
            processed_count += 1
        except Exception as e:
            print(f"\n✗ 에러 발생: {e}")
            print(f"   파일: {input_path}")
            continue

    print("\n\n" + "=" * 60)
    print("전체 전처리 파이프라인 완료!")
    print(f"  - 처리된 파일: {processed_count}개")
    print(f"  - 건너뛴 파일: {skipped_count}개")
    print("=" * 60)


if __name__ == "__main__":
    main()
