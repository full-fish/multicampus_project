"""
전처리 파이프라인에서 사용되는 유틸리티 함수들
"""

import os
import re
import glob
import pickle
import numpy as np
import pandas as pd
from konlpy.tag import Okt

# 형태소 분석기 초기화
okt = Okt()


def load_stopwords(filename="stopwords-ko.txt"):
    """불용어 로드"""
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        stopword_path = os.path.join(base_dir, filename)
        with open(stopword_path, "r", encoding="utf-8") as f:
            return set([line.strip() for line in f if line.strip()])
    except FileNotFoundError:
        print(f"\n[오류] 불용어 파일이 없습니다: {stopword_path}")
        print("작업을 중단합니다. 파일을 확인해주세요.")
        raise
    except Exception as e:
        print(f"\n[오류] 파일 읽기 중 알 수 없는 문제가 발생했습니다: {e}")
        raise


def get_tokens(text, stopwords):
    """텍스트를 토큰화"""
    if not isinstance(text, str):
        return []
    clean_text = re.sub(r"[^가-힣0-9\s]", " ", text)
    clean_text = re.sub(r"\s+", " ", clean_text).strip()

    tokens = []
    for word, pos in okt.pos(clean_text, stem=True):
        if pos in ("Noun", "Verb", "Adjective") and word not in stopwords:
            tokens.append(word)
    return tokens


def cosine_similarity(vec1, vec2):
    """두 벡터 간 코사인 유사도 계산"""
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)

    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return np.dot(vec1, vec2) / (norm1 * norm2)


class TokenIterator:
    """Word2Vec 학습을 위한 토큰 Iterator (메모리 효율적)"""

    def __init__(self, token_dir):
        self.token_dir = token_dir
        self.token_files = glob.glob(os.path.join(token_dir, "*.pkl"))

    def __iter__(self):
        for token_file in self.token_files:
            try:
                with open(token_file, "rb") as f:
                    file_tokens = pickle.load(f)
                    for tokens in file_tokens:
                        if tokens:  # 빈 토큰 리스트는 제외
                            yield tokens
            except Exception as e:
                print(f"  [경고] 토큰 파일 읽기 실패: {token_file} - {e}")
                continue


# =========================
# Parquet 파일 로딩 함수
# =========================


def load_products_parquet(
    parquet_path="./data/processed_data/integrated_products_vector.parquet",
):
    """
    상품 벡터 Parquet 파일 로드

    Args:
        parquet_path: Parquet 파일 경로

    Returns:
        DataFrame: 상품 정보 (product_vector, representative_review_id 포함)
    """
    try:
        df = pd.read_parquet(parquet_path)
        return df
    except FileNotFoundError:
        print(f"[오류] 파일을 찾을 수 없습니다: {parquet_path}")
        return None
    except Exception as e:
        print(f"[오류] Parquet 파일 읽기 실패: {e}")
        return None


def load_reviews_parquet(
    parquet_path="./data/processed_data/integrated_reviews_detail.parquet",
    product_id=None,
    category=None,
):
    """
    리뷰 상세 Parquet 파일 로드 (필터링 옵션)

    Args:
        parquet_path: Parquet 파일 경로
        product_id: 특정 상품 ID로 필터링 (None이면 전체 로드)
        category: 카테고리로 추가 필터링 (product_id 중복 방지)

    Returns:
        DataFrame: 리뷰 상세 정보 (tokens, word2vec 포함)

    Note:
        product_id는 카테고리별로 중복될 수 있으므로,
        정확한 필터링을 위해 category와 함께 사용하는 것을 권장합니다.
    """
    try:
        filters = []

        if product_id and category:
            # product_id + category 조합으로 필터링 (권장)
            df = pd.read_parquet(parquet_path)
            df = df[(df["product_id"] == product_id)]
            # category 정보가 리뷰 테이블에 없을 수 있으므로, 상품 테이블과 조인 필요
            return df
        elif product_id:
            # product_id만으로 필터링 (중복 가능성 있음)
            print(
                "[경고] product_id는 카테고리별로 중복될 수 있습니다. category도 함께 지정하세요."
            )
            df = pd.read_parquet(parquet_path)
            df = df[df["product_id"] == product_id]
            return df
        else:
            # 전체 리뷰 로드
            df = pd.read_parquet(parquet_path)
            return df

        print(f"[오류] 파일을 찾을 수 없습니다: {parquet_path}")
        return None
    except Exception as e:
        print(f"[오류] Parquet 파일 읽기 실패: {e}")
        return None


def load_reviews_by_products(
    product_ids, parquet_path="./data/processed_data/integrated_reviews_detail.parquet"
):
    """
    여러 상품의 리뷰를 한 번에 로드

    Args:
        product_ids: 상품 ID 리스트
        parquet_path: Parquet 파일 경로

    Returns:
        DataFrame: 필터링된 리뷰 데이터
    """
    try:
        df = pd.read_parquet(parquet_path, filters=[("product_id", "in", product_ids)])
        return df
    except FileNotFoundError:
        print(f"[오류] 파일을 찾을 수 없습니다: {parquet_path}")
        return None
    except Exception as e:
        print(f"[오류] Parquet 파일 읽기 실패: {e}")
        return None
