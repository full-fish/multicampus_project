"""
전처리 파이프라인 Phase 1, 2, 3 함수들
"""

import json
import os
import glob
import pickle
import warnings
import sys
from contextlib import contextmanager
import numpy as np
from gensim.models import Word2Vec
from multiprocessing import cpu_count
from preprocess_format import preprocess_format
from brand_standardizer import brand_standardizer
from drop_missing_val_splitter import drop_missing_val_splitter
from preprocessing_utils import (
    load_stopwords,
    get_tokens,
    cosine_similarity,
    TokenIterator,
)

# gensim 내부 경고 억제
warnings.filterwarnings("ignore", category=RuntimeWarning, module="gensim")


@contextmanager
def suppress_stderr():
    """stderr 출력을 임시로 억제"""
    original_stderr = sys.stderr
    sys.stderr = open(os.devnull, "w")
    try:
        yield
    finally:
        sys.stderr.close()
        sys.stderr = original_stderr


MAX_WORKERS = max(1, cpu_count() - 1)


def preprocess_and_tokenize_file(args):
    """
    Phase 1: 파일 전처리 + 토큰화 (병렬 실행)
    - 포맷 전처리, 브랜드 표준화, 결측치 제거, 토큰화를 한 번에 수행
    - 토큰 결과를 임시 파일로 저장
    """
    input_path, pre_data_dir, processed_data_dir, temp_tokens_dir = args

    file_name = os.path.basename(input_path)
    stopwords = load_stopwords()

    try:
        # 상대 경로 계산
        rel_path = os.path.relpath(input_path, pre_data_dir)
        rel_dir = os.path.dirname(rel_path)
        output_dir = os.path.join(processed_data_dir, rel_dir)

        # 출력 파일명 계산
        base_name = os.path.splitext(file_name)[0]
        if base_name.startswith("result_"):
            base_name = base_name[7:]

        output_with_text = os.path.join(
            output_dir, f"processed_{base_name}_with_text.json"
        )
        output_without_text = os.path.join(
            output_dir, f"processed_{base_name}_without_text.json"
        )

        # 이미 처리된 파일이면 스킵
        if os.path.exists(output_with_text) and os.path.exists(output_without_text):
            return {"status": "skipped", "file": file_name}

        # 1. JSON 파일 로드
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 2. 포맷 전처리
        temp_file = f"temp_{os.getpid()}_{base_name}.json"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        data = preprocess_format(temp_file)

        if os.path.exists(temp_file):
            os.remove(temp_file)

        # 3. 브랜드 표준화
        data = brand_standardizer(data)

        # 4. 결측치 제거 및 분할
        with_text, without_text = drop_missing_val_splitter(data)

        # 5. 토큰화 (한 번만 수행하고 저장)
        all_tokens = []  # Word2Vec 학습용
        tokenized_data = []  # 나중에 벡터화에 사용할 토큰 저장

        for product_idx, product in enumerate(with_text.get("data", [])):
            p_info = product.get("product_info", {})

            # product_id를 전역적으로 고유하게 만들기 (카테고리_원본ID)
            original_id = p_info.get("product_id", p_info.get("id", ""))
            category = base_name  # 파일명이 카테고리
            unique_product_id = f"{category}_{original_id}"

            # product_info에 고유 ID 업데이트
            p_info["product_id"] = unique_product_id
            p_info["original_product_id"] = original_id  # 원본 ID 보존
            p_info["category_file"] = category

            product_tokens = {
                "product_id": unique_product_id,
                "reviews": [],
            }

            for review in product.get("reviews", {}).get("data", []):
                full_text = review.get("full_text", "")
                tokens = get_tokens(full_text, stopwords)

                # 토큰 저장 (글자 수, 토큰 수 포함)
                product_tokens["reviews"].append(
                    {
                        "review_id": review.get("id"),
                        "tokens": tokens,
                        "score": review.get("score", 3),
                        "char_length": len(full_text),
                        "token_count": len(tokens),
                    }
                )

                if tokens:
                    all_tokens.append(tokens)

            tokenized_data.append(product_tokens)

        # 6. 토큰을 임시 파일로 저장 (Word2Vec 학습용)
        os.makedirs(temp_tokens_dir, exist_ok=True)
        token_file = os.path.join(temp_tokens_dir, f"{base_name}_tokens.pkl")
        with open(token_file, "wb") as f:
            pickle.dump(all_tokens, f)

        # 7. 토큰화된 데이터 저장 (벡터화에 재사용)
        tokenized_file = os.path.join(temp_tokens_dir, f"{base_name}_tokenized.pkl")
        with open(tokenized_file, "wb") as f:
            pickle.dump(
                {
                    "with_text": with_text,
                    "without_text": without_text,
                    "tokenized_data": tokenized_data,
                },
                f,
            )

        return {
            "status": "success",
            "file": file_name,
            "token_count": len(all_tokens),
            "output_dir": output_dir,
            "base_name": base_name,
        }

    except Exception as e:
        return {"status": "error", "file": file_name, "error": str(e)}


def train_global_word2vec(temp_tokens_dir):
    """
    Phase 2: Iterator 방식으로 Word2Vec 모델 학습 (메모리 효율적)
    """
    print("\n" + "=" * 60)
    print("전역 Word2Vec 모델 학습 시작 (Iterator 방식)")
    print("=" * 60)

    # TokenIterator를 사용하여 메모리에 모든 토큰을 올리지 않음
    token_iterator = TokenIterator(temp_tokens_dir)

    # 토큰 파일 개수 확인
    token_files = glob.glob(os.path.join(temp_tokens_dir, "*_tokens.pkl"))
    print(f"토큰 파일 수: {len(token_files)}개")

    if not token_files:
        print("[경고] 토큰 파일이 없습니다. Word2Vec 학습을 건너뜁니다.")
        return None

    # Word2Vec 모델 학습 (Skip-gram, Iterator 방식) - stderr 억제
    with suppress_stderr():
        model = Word2Vec(
            sentences=token_iterator,
            vector_size=100,
            window=5,
            min_count=3,
            workers=MAX_WORKERS,
            sg=1,  # Skip-gram
        )

    print(f"Word2Vec 모델 학습 완료 (어휘 크기: {len(model.wv):,})")
    return model


def vectorize_file(args):
    """
    Phase 3: 저장된 토큰을 재사용하여 벡터화 + 대표 리뷰 선정 (병렬 실행)
    - JSON: 상품 요약 정보만 저장 (대표 벡터 포함)
    - 리뷰 상세 정보는 반환하여 Parquet로 통합 저장
    """
    base_name, temp_tokens_dir, output_dir, w2v_model = args

    try:
        # 저장된 토큰화 데이터 로드
        tokenized_file = os.path.join(temp_tokens_dir, f"{base_name}_tokenized.pkl")
        with open(tokenized_file, "rb") as f:
            saved_data = pickle.load(f)

        with_text = saved_data["with_text"]
        without_text = saved_data["without_text"]
        tokenized_data = saved_data["tokenized_data"]

        # 상품 요약 정보 & 리뷰 상세 정보 수집
        product_summaries = []
        review_details = []

        for product_idx, product in enumerate(with_text.get("data", [])):
            review_vectors = []
            product_tokens = tokenized_data[product_idx]
            product_info = product.get("product_info", {})

            for review_idx, review in enumerate(
                product.get("reviews", {}).get("data", [])
            ):
                # 저장된 토큰 재사용
                saved_review = product_tokens["reviews"][review_idx]
                tokens = saved_review["tokens"]
                score = saved_review["score"]

                # 감성 라벨링
                if score >= 4:
                    label = 1  # 긍정
                elif score <= 2:
                    label = 0  # 부정
                else:
                    label = None  # 중립

                # 리뷰 벡터 생성
                word_vectors = [w2v_model.wv[w] for w in tokens if w in w2v_model.wv]
                if word_vectors:
                    review_vec = np.mean(word_vectors, axis=0)
                    review_vectors.append(
                        {
                            "vector": review_vec,
                            "review_id": review.get("id"),
                            "review_idx": review_idx,
                        }
                    )
                else:
                    review_vec = np.zeros(100)  # 빈 벡터

                # 리뷰 상세 정보 수집 (Parquet 저장용)
                review_details.append(
                    {
                        "product_id": product_info.get("product_id"),
                        "review_id": review.get("id"),
                        "full_text": review.get("full_text", ""),
                        "score": score,
                        "label": label,
                        "tokens": tokens,
                        "word2vec": review_vec.tolist(),
                        "char_length": saved_review["char_length"],
                        "token_count": saved_review["token_count"],
                        "date": review.get("date"),
                        "nickname": review.get("nickname"),
                        "has_image": review.get("has_image"),
                        "helpful_count": review.get("helpful_count"),
                    }
                )

            # 상품 대표 벡터 생성 (모든 리뷰의 평균)
            if review_vectors:
                # 상품 센트로이드 벡터
                product_vec = np.mean([rv["vector"] for rv in review_vectors], axis=0)
                product_info["product_vector"] = product_vec.tolist()

                # 대표 리뷰 선정: 센트로이드와 코사인 유사도가 가장 높은 리뷰
                max_similarity = -1
                representative_review_id = None

                for rv in review_vectors:
                    similarity = cosine_similarity(product_vec, rv["vector"])
                    if similarity > max_similarity:
                        max_similarity = similarity
                        representative_review_id = rv["review_id"]

                product_info["representative_review_id"] = representative_review_id
                product_info["representative_similarity"] = float(max_similarity)
            else:
                product_info["product_vector"] = []
                product_info["representative_review_id"] = None
                product_info["representative_similarity"] = 0.0

            product_summaries.append(product_info)

        # 결과 저장
        os.makedirs(output_dir, exist_ok=True)

        # JSON: 상품 요약 정보만 저장 (리뷰 제외, 메타데이터 포함)
        processed_with_text = os.path.join(
            output_dir, f"processed_{base_name}_with_text.json"
        )
        processed_without_text = os.path.join(
            output_dir, f"processed_{base_name}_without_text.json"
        )

        # with_text: 메타데이터 포함하여 저장
        with_text_processed = {
            "search_name": with_text.get("search_name", ""),
            "total_collected_reviews": with_text.get("total_collected_reviews", 0),
            "total_text_reviews": with_text.get("total_text_reviews", 0),
            "total_product": len(product_summaries),
            "total_rating_distribution": with_text.get("total_rating_distribution", {}),
            "data": product_summaries,
        }

        with open(processed_with_text, "w", encoding="utf-8") as f:
            json.dump(with_text_processed, f, ensure_ascii=False, indent=2)

        with open(processed_without_text, "w", encoding="utf-8") as f:
            json.dump(without_text, f, ensure_ascii=False, indent=2)

        return {
            "status": "success",
            "file": base_name,
            "product_summaries": product_summaries,
            "review_details": review_details,
        }

    except Exception as e:
        return {"status": "error", "file": base_name, "error": str(e)}
