"""
전처리 파이프라인 메인 orchestrator
"""
import json
import os
import glob
import time
from datetime import datetime
import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from preprocessing_phases import (
    preprocess_and_tokenize_file,
    train_global_word2vec,
    vectorize_file,
    MAX_WORKERS,
)

# 임시 토큰 저장 디렉토리
TEMP_TOKENS_DIR = "./data/temp_tokens"


def main():
    """
    최적화된 전처리 파이프라인:
    Phase 1: 병렬 전처리 + 토큰화 (1회만)
    Phase 2: Iterator 방식 Word2Vec 학습 (메모리 효율적)
    Phase 3: 병렬 벡터화 + 대표 리뷰 선정
    """
    # 시작 시간 기록
    start_time = time.time()
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    PRE_DATA_DIR = "./data/pre_data"
    PROCESSED_DATA_DIR = "./data/processed_data"
    PRODUCT_PARQUET = "./data/processed_data/integrated_products_vector.parquet"
    REVIEW_PARQUET = "./data/processed_data/integrated_reviews_detail.parquet"

    print("\n" + "=" * 60)
    print(f"{'최적화된 전처리 파이프라인 시작':^60}")
    print(f"{'시작 시간: ' + start_datetime:^60}")
    print("=" * 60 + "\n")

    # pre_data 디렉토리의 모든 JSON 파일 찾기
    json_files = glob.glob(os.path.join(PRE_DATA_DIR, "**", "*.json"), recursive=True)

    if not json_files:
        print(f"\n[오류] {PRE_DATA_DIR} 디렉토리에서 JSON 파일을 찾을 수 없습니다.")
        return

    print(f"총 {len(json_files)}개 파일 발견")
    print(f"병렬 처리 워커 수: {MAX_WORKERS}개\n")

    # ========== Phase 1: 병렬 전처리 + 토큰화 ==========
    print("=" * 60)
    print("Phase 1: 전처리 및 토큰화 (병렬 처리)")
    print("=" * 60)

    phase1_start = time.time()

    # 임시 디렉토리 생성
    os.makedirs(TEMP_TOKENS_DIR, exist_ok=True)

    # 병렬로 전처리 + 토큰화 실행
    args_list = [
        (input_path, PRE_DATA_DIR, PROCESSED_DATA_DIR, TEMP_TOKENS_DIR)
        for input_path in json_files
    ]

    skipped_count = 0
    phase1_results = []

    with Pool(MAX_WORKERS) as pool:
        for result in tqdm(
            pool.imap_unordered(preprocess_and_tokenize_file, args_list),
            total=len(json_files),
            desc="전처리 및 토큰화",
            unit="파일",
        ):
            if result["status"] == "skipped":
                skipped_count += 1
                tqdm.write(f"  [건너뜀] {result['file']}")
            elif result["status"] == "success":
                phase1_results.append(result)
                tqdm.write(
                    f"  [완료] {result['file']} - 토큰: {result['token_count']:,}개"
                )
            else:
                tqdm.write(
                    f"  [에러] {result['file']} - {result.get('error', 'Unknown')}"
                )

    phase1_time = time.time() - phase1_start
    print(f"\nPhase 1 완료 - 소요 시간: {phase1_time:.2f}초")
    print(f"  처리 완료: {len(phase1_results)}개")
    print(f"  건너뜀: {skipped_count}개\n")

    # ========== Phase 2: Word2Vec 학습 ==========
    phase2_start = time.time()
    w2v_model = train_global_word2vec(TEMP_TOKENS_DIR)
    phase2_time = time.time() - phase2_start
    print(f"Phase 2 완료 - 소요 시간: {phase2_time:.2f}초\n")

    if not w2v_model:
        print("[오류] Word2Vec 모델 학습 실패")
        return

    # ========== Phase 3: 병렬 벡터화 + 대표 리뷰 선정 ==========
    print("=" * 60)
    print("Phase 3: 벡터화 및 대표 리뷰 선정 (병렬 처리)")
    print("=" * 60)

    phase3_start = time.time()

    # Phase 1에서 처리된 파일들에 대해 벡터화 실행
    vectorize_args = [
        (result["base_name"], TEMP_TOKENS_DIR, result["output_dir"], w2v_model)
        for result in phase1_results
    ]

    all_products = []
    all_reviews = []

    with Pool(MAX_WORKERS) as pool:
        for result in tqdm(
            pool.imap_unordered(vectorize_file, vectorize_args),
            total=len(vectorize_args),
            desc="벡터화 및 대표 리뷰 선정",
            unit="파일",
        ):
            if result["status"] == "success":
                all_products.extend(result["product_summaries"])
                all_reviews.extend(result["review_details"])
                tqdm.write(f"  [완료] {result['file']}")
            else:
                tqdm.write(
                    f"  [에러] {result['file']} - {result.get('error', 'Unknown')}"
                )

    # 건너뛴 파일의 상품 정보도 로드
    if skipped_count > 0:
        print(f"\n건너뛴 파일 {skipped_count}개의 데이터 로드 중...")
        for input_path in json_files:
            rel_path = os.path.relpath(input_path, PRE_DATA_DIR)
            rel_dir = os.path.dirname(rel_path)
            output_dir = os.path.join(PROCESSED_DATA_DIR, rel_dir)

            file_name = os.path.basename(input_path)
            base_name = os.path.splitext(file_name)[0]
            if base_name.startswith("result_"):
                base_name = base_name[7:]

            processed_file = os.path.join(
                output_dir, f"processed_{base_name}_with_text.json"
            )

            if os.path.exists(processed_file):
                try:
                    with open(processed_file, "r", encoding="utf-8") as f:
                        existing_data = json.load(f)
                    all_products.extend(existing_data.get("data", []))
                except:
                    pass

    phase3_time = time.time() - phase3_start
    print(f"\nPhase 3 완료 - 소요 시간: {phase3_time:.2f}초\n")

    # ========== Parquet 파일 생성 ==========
    print("=" * 60)
    print("Parquet 파일 생성 중...")
    print("=" * 60)

    # 1. 상품 벡터 Parquet (요약 정보)
    if all_products:
        df_products = pd.DataFrame(all_products)
        df_products.to_parquet(
            PRODUCT_PARQUET, engine="pyarrow", compression="snappy", index=False
        )

        product_size_mb = os.path.getsize(PRODUCT_PARQUET) / 1024 / 1024
        print(f"✓ 상품 Parquet 저장: {PRODUCT_PARQUET}")
        print(f"  - 상품 수: {len(df_products):,}개")
        print(f"  - 파일 크기: {product_size_mb:.2f} MB\n")

    # 2. 리뷰 상세 Parquet (토큰, 벡터 포함)
    if all_reviews:
        df_reviews = pd.DataFrame(all_reviews)
        df_reviews.to_parquet(
            REVIEW_PARQUET, engine="pyarrow", compression="snappy", index=False
        )

        review_size_mb = os.path.getsize(REVIEW_PARQUET) / 1024 / 1024
        print(f"✓ 리뷰 Parquet 저장: {REVIEW_PARQUET}")
        print(f"  - 리뷰 수: {len(df_reviews):,}개")
        print(f"  - 파일 크기: {review_size_mb:.2f} MB")

    # ========== 임시 파일 정리 ==========
    print(f"\n임시 토큰 파일 정리 중...")
    try:
        import shutil

        shutil.rmtree(TEMP_TOKENS_DIR)
        print(f"임시 디렉토리 삭제 완료: {TEMP_TOKENS_DIR}")
    except Exception as e:
        print(f"[경고] 임시 디렉토리 삭제 실패: {e}")

    # 종료 시간 및 소요 시간 계산
    end_time = time.time()
    end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elapsed_time = end_time - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    print("\n" + "=" * 60)
    print(f"{'전체 파이프라인 완료!':^60}")
    print(f"{'종료 시간: ' + end_datetime:^60}")
    print(f"{'총 소요 시간: ' + f'{hours}시간 {minutes}분 {seconds}초':^60}")
    print(
        f"{'Phase 1: ' + f'{phase1_time:.1f}초 | Phase 2: {phase2_time:.1f}초 | Phase 3: {phase3_time:.1f}초':^60}"
    )
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
