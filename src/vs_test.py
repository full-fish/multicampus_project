import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
from itertools import chain
import seaborn as sns
import random
import matplotlib.gridspec as gridspec
from pathlib import Path

from matplotlib import rc
import platform

# 운영체제별 한글 폰트 설정
if platform.system() == "Windows":
    plt.rc("font", family="Malgun Gothic")
    plt.rcParams["axes.unicode_minus"] = False
    FONT_PATH = r"C:\WINDOWS\FONTS\MALGUNSL.TTF"
elif platform.system() == "Darwin":  # macOS
    plt.rc("font", family="AppleGothic")
    plt.rcParams["axes.unicode_minus"] = False
    FONT_PATH = "/System/Library/Fonts/Supplemental/AppleGothic.ttf"
else:  # Linux
    plt.rc("font", family="NanumGothic")
    plt.rcParams["axes.unicode_minus"] = False
    FONT_PATH = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"

# 파일 경로
DATA_DIR = "data/processed_data/"
PARQUET_PATH = "data/processed_data/integrated_reviews_detail.parquet"

# with_text와 without_text 파일 모두 재귀적으로 수집
data_path = Path(DATA_DIR)
with_text_files = list(data_path.rglob("*_with_text.json"))
without_text_files = list(data_path.rglob("*_without_text.json"))
all_json_files = with_text_files + without_text_files

print(f"with_text 파일: {len(with_text_files)}개")
print(f"without_text 파일: {len(without_text_files)}개")
print(f"총 파일 개수: {len(all_json_files)}개")

# 1. Parquet에서 리뷰 데이터 로드 (텍스트, 토큰, 벡터 등)
print("\n리뷰 데이터 로딩 중...")
df_reviews = pd.read_parquet(PARQUET_PATH)
print(f"총 리뷰 수: {len(df_reviews)}")

# 2. JSON에서 상품 정보 로드 (with_text + without_text)
print("\n상품 정보 로딩 중...")
product_rows = []

for path in all_json_files:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    file_type = "with_text" if "with_text" in str(path) else "without_text"
    print(f"불러오는 파일: {path.name} ({file_type})")

    for product in raw.get("data", []):
        p_info = product.get("product_info", {})

        # without_text 파일의 경우 평점 분포 정보 추출
        reviews_info = product.get("reviews", {})

        product_rows.append(
            {
                "source_file": path.name,
                "file_type": file_type,
                "product_id": p_info.get("product_id"),
                "product_name": p_info.get("product_name"),
                "brand": p_info.get("brand"),
                "category_path": p_info.get("category_path"),
                "category_normal": p_info.get("category_normal"),
                "price": pd.to_numeric(p_info.get("price"), errors="coerce"),
                "total_reviews": p_info.get("total_reviews", 0),
                "rating_distribution": p_info.get("rating_distribution", {}),
                "skin_type": p_info.get("skin_type"),
            }
        )

df_products = pd.DataFrame(product_rows)

# 3. 리뷰 데이터와 상품 정보 병합
print("\n데이터 병합 중...")
df = df_reviews.merge(
    df_products[["product_id", "product_name", "brand", "category_path", "price"]],
    on="product_id",
    how="left",
)

print("\n===== 병합된 데이터프레임 =====")
print(df.head())
print(df.info())


df["has_image"] = df["has_image"].fillna(0).astype(int)
df["helpful_count"] = df["helpful_count"].fillna(0).astype(int)
df["review_len"] = df["full_text"].astype(str).apply(len)
df["date"] = pd.to_datetime(df["date"], errors="coerce")

# 전체 상품 및 리뷰 통계
print("\n===== 전체 통계 =====")
print(f"총 상품 수 (with_text + without_text): {len(df_products)}")
print(f"with_text 상품 수: {len(df_products[df_products['file_type'] == 'with_text'])}")
print(
    f"without_text 상품 수: {len(df_products[df_products['file_type'] == 'without_text'])}"
)
print(f"총 리뷰 수 (텍스트 포함): {len(df)}")


# 리뷰 많은 상품 TOP 5
top_5_products = (
    df.groupby(["product_id", "product_name"])
    .size()
    .reset_index(name="review_count")
    .sort_values("review_count", ascending=False)
    .head(5)
)

print("\n===== 리뷰 많은 상품 TOP 5 =====")
print(top_5_products)

# 텍스트 있는 리뷰
df_text = df[df["review_len"] > 0].copy()
df_all = df.copy()

print(f"전체 리뷰 수: {len(df_all)}")
print(f"텍스트 리뷰 수: {len(df_text)}")


# 평점 분포
print("\n===== 평점 분포 =====")
print(df["score"].value_counts().sort_index())

# 리뷰 길이
print("\n===== 리뷰 길이 통계 =====")
print(df["review_len"].describe())


# 상품별 평균 평점
product_score = (
    df[df["product_name"].notna()]  # product_name이 있는 것만
    .groupby("product_name")
    .agg(
        mean_score=("score", "mean"),
        mean_helpful=("helpful_count", "mean"),
        review_count=("score", "count"),
    )
    .reset_index()
)

print("\n===== 상품별 평균 평점 & 평균 helpful_count =====")
print(f"상품 수: {len(product_score)}")
print(product_score.head())

# 리뷰 수 TOP 10 상품
top_products = (
    df[df["product_name"].notna()]["product_name"].value_counts().head(10).index
)
print(f"\nTOP 10 상품 수: {len(top_products)}")


# 평점별 helpful_count
print("\n===== 평점별 helpful_count 통계 =====")
print(df.groupby("score")["helpful_count"].describe())

# 평점별 평균 리뷰 길이
print("\n===== 평점별 평균 리뷰 길이 =====")
print(df.groupby("score")["review_len"].mean())

# 평점별 리뷰 수 비율
print("\n===== 평점별 리뷰 수 비율 =====")
print(df["score"].value_counts(normalize=True).sort_index())


# 상품별 리뷰 수 분포
print("\n===== 상품별 리뷰 수 통계 =====")
print(df["product_id"].value_counts().describe())


# 상관계수
print("\n===== 상관계수 =====")
print("score - helpful_count :", df["score"].corr(df["helpful_count"]))
print("score - has_image :", df["score"].corr(df["has_image"]))

corr_product = product_score["mean_score"].corr(product_score["mean_helpful"])
print("상품 평균 평점 - 상품 평균 helpful_count :", corr_product)


# 시각화 1
fig, axes = plt.subplots(2, 3, figsize=(18, 10))

df["score"].value_counts().sort_index().plot(kind="bar", ax=axes[0, 0])
axes[0, 0].set_title("평점 분포")

axes[0, 1].hist(df["review_len"], bins=50)
axes[0, 1].set_title("리뷰 길이 분포")

axes[0, 2].scatter(df["review_len"], df["helpful_count"], alpha=0.3)
axes[0, 2].set_xscale("log")
axes[0, 2].set_yscale("log")
axes[0, 2].set_title("리뷰 길이 vs Helpful Count")

sns.violinplot(x="score", y="review_len", data=df, ax=axes[1, 0])
axes[1, 0].set_title("평점별 리뷰 길이")

sns.boxplot(x="score", y="helpful_count", data=df, ax=axes[1, 1])
axes[1, 1].set_yscale("log")
axes[1, 1].set_title("평점별 Helpful Count")

# 상품 평균 평점 vs 평균 Helpful - 안전하게 처리
if len(product_score) > 0:
    axes[1, 2].scatter(
        product_score["mean_score"], product_score["mean_helpful"], alpha=0.5
    )
    axes[1, 2].set_xlabel("평균 평점")
    axes[1, 2].set_ylabel("평균 Helpful Count")
    axes[1, 2].set_title("상품 평균 평점 vs 평균 Helpful")
else:
    axes[1, 2].text(
        0.5,
        0.5,
        "데이터 없음",
        ha="center",
        va="center",
        transform=axes[1, 2].transAxes,
    )
    axes[1, 2].set_title("상품 평균 평점 vs 평균 Helpful")

plt.tight_layout()
plt.show()


# 시각화 2
fig = plt.figure(figsize=(16, 8))
gs = gridspec.GridSpec(2, 3)

ax1 = fig.add_subplot(gs[0, 0])
# TOP 10 상품이 있을 때만 그리기
if len(top_products) > 0:
    top_product_scores = (
        df[df["product_name"].isin(top_products)]
        .groupby("product_name")["score"]
        .mean()
        .sort_values()
    )
    if len(top_product_scores) > 0:
        top_product_scores.plot(kind="barh", ax=ax1)
        ax1.set_title("TOP 10 상품 평균 평점")
        ax1.set_xlabel("평균 평점")
    else:
        ax1.text(
            0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax1.transAxes
        )
        ax1.set_title("TOP 10 상품 평균 평점")
else:
    ax1.text(0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax1.transAxes)
    ax1.set_title("TOP 10 상품 평균 평점")

ax2 = fig.add_subplot(gs[0, 2])
# pivot_table에서 review 대신 review_id 사용 (또는 full_text)
if len(top_products) > 0:
    pivot = df[df["product_name"].isin(top_products)].pivot_table(
        index="product_name",
        columns="score",
        values="review_id",
        aggfunc="count",
        fill_value=0,
    )
    if not pivot.empty:
        sns.heatmap(pivot, annot=True, fmt=".0f", cmap="YlOrRd", ax=ax2)
        ax2.set_title("TOP 10 상품 평점 분포")
        ax2.set_xlabel("평점")
        ax2.set_ylabel("상품명")
    else:
        ax2.text(
            0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax2.transAxes
        )
        ax2.set_title("TOP 10 상품 평점 분포")
else:
    ax2.text(0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax2.transAxes)
    ax2.set_title("TOP 10 상품 평점 분포")

ax3 = fig.add_subplot(gs[1, :])
# 날짜 데이터가 있을 때만 그리기
if df["date"].notna().sum() > 0:
    time_score = (
        df.dropna(subset=["date"]).set_index("date").resample("ME")["score"].mean()
    )
    if len(time_score) > 0:
        time_score.plot(ax=ax3, linewidth=2)
        ax3.set_title("월별 평균 평점 추이")
    else:
        ax3.text(0.5, 0.5, "시계열 데이터 부족", ha="center", va="center")
        ax3.set_title("월별 평균 평점 추이")
else:
    ax3.text(0.5, 0.5, "날짜 데이터 없음", ha="center", va="center")
    ax3.set_title("월별 평균 평점 추이")

plt.tight_layout()
plt.show()


# ===== 워드클라우드(수정중) =====
