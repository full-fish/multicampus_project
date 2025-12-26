import json
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
from itertools import chain

from matplotlib import rc 
import platform 

if platform.system() == "Windows": 
    plt.rc('font', family="Malgun Gothic") 
    plt.rcParams['axes.unicode_minus'] = False


JSON_PATH = "project\prep\메이크업\베이스 메이크업\processed_컨실러_with_text.json"

with open(JSON_PATH, "r", encoding="utf-8") as f:
    raw = json.load(f)

print("검색 키워드:", raw.get("search_name"))
print("총 리뷰 수:", raw.get("total_collected_reviews"))
print("총 상품 수:", raw.get("total_product"))


products = raw["data"]

# 리뷰 많은 상품 TOP 5
top_5_products = sorted(
    products,
    key=lambda x: x["product_info"]["total_reviews"],
    reverse=True
)[:5]


# 폰트 경로(맞는걸로 바꾸기)
FONT_PATH = r"C:\WINDOWS\FONTS\MALGUNSL.TTF"


# 리뷰 단위 데이터프레임 생성
rows = []

for product in raw.get("data"):
    p_info = product["product_info"]
    reviews = product["reviews"]["data"]

    for r in reviews:
        rows.append({
            "product_id": p_info.get("product_id"),
            "product_name": p_info.get("product_name_clean"),
            "category": p_info.get("category_norm"),
            "price": p_info.get("price"),
            "score": r.get("score"),
            "review_title": r.get("title", ""),
            "review": r.get("content", ""),
            "full_text": r.get("full_text", ""),
            "tokens": r.get("tokens", []),
            "helpful_count": r.get("helpful_count", 0),
            "label": r.get("label"),
            "review_date": r.get("date")
        })

df = pd.DataFrame(rows)

print("\n===== 데이터프레임 =====")
print(df.head())
print(df.info())


# 평점 분포
print("\n===== 평점 분포 =====")
print(df["score"].value_counts().sort_index())

df["score"].value_counts().sort_index().plot(kind="bar")
plt.title("평점 분포")
plt.xlabel("평점")
plt.ylabel("개수")
plt.show()


# 리뷰 길이
df["review_len"] = df["full_text"].astype(str).apply(len)


print("\n===== 리뷰 길이 통계 =====")
print(df["review_len"].describe())

plt.hist(df["review_len"], bins=50)
plt.title("리뷰 길이 분포")
plt.xlabel("리뷰 길이")
plt.ylabel("개수")
plt.show()


# 평점별 도움이 됐어요 분포
print("\n===== 평점별 helpful_count 통계 =====")
print(df.groupby("score")["helpful_count"].describe())


# 평점별 평균 리뷰 길이
print("\n===== 평점별 평균 리뷰 길이 =====")
print(df.groupby("score")["review_len"].mean())


# 리뷰 존재 여부 vs 평점
df["has_review"] = df["review"].notnull()

print("\n===== 평점별 리뷰 수 비율 =====")
print(df["score"].value_counts(normalize=True).sort_index())


# 전체 키워드 빈도
all_tokens = []
for t in df["tokens"]:
    all_tokens.extend(t)

print("\n===== 전체 키워드 TOP 20 =====")
print(Counter(all_tokens).most_common(20))


# 긍/부정 리뷰 키워드 비교
pos_tokens = list(chain.from_iterable(
    df.loc[df["label"] == 1, "tokens"]
))

neg_tokens = list(chain.from_iterable(
    df.loc[df["label"] == 0, "tokens"]
))

print("\n===== 긍정 리뷰 키워드 TOP 10 =====")
print(Counter(pos_tokens).most_common(10))

print("\n===== 부정 리뷰 키워드 TOP 10 =====")
print(Counter(neg_tokens).most_common(10))


# 상품별 리뷰 수 분포
print("\n===== 상품별 리뷰 수 통계 =====")
print(df["product_id"].value_counts().describe())


# 긍/부정 키워드 워드클라우드
pos_text = " ".join(pos_tokens)
neg_text = " ".join(neg_tokens)

wc_pos = WordCloud(
    font_path=FONT_PATH,
    background_color="white",
    width=800,
    height=600,
    max_words=100
).generate(pos_text)

plt.figure(figsize=(8, 6))
plt.imshow(wc_pos)
plt.axis("off")
plt.title("긍정 리뷰 워드클라우드")
plt.show()

wc_neg = WordCloud(
    font_path=FONT_PATH,
    background_color="white",
    width=800,
    height=600,
    max_words=100
).generate(neg_text)

plt.figure(figsize=(8, 6))
plt.imshow(wc_neg)
plt.axis("off")
plt.title("부정 리뷰 워드클라우드")
plt.show()
