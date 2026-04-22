import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import sqlalchemy

# ── Connection ─────────────────────────────────────────────────────────────────
engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://admin:admin123@localhost:5432/tp_ddev_1"
)

# ── Queries ────────────────────────────────────────────────────────────────────
rev_query = """
SELECT d.year, d.month, d.month_name, SUM(f.price) AS revenue
FROM data_warehouse.fact_sales f
JOIN data_warehouse.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
"""

cat_query = """
SELECT p.product_category_en AS category, SUM(f.price) AS revenue
FROM data_warehouse.fact_sales f
JOIN data_warehouse.dim_product p ON f.product_sk = p.product_sk
WHERE p.product_category_en IS NOT NULL
GROUP BY p.product_category_en
ORDER BY revenue DESC
LIMIT 10;
"""

score_query = """
SELECT review_score, COUNT(*) AS cnt
FROM data_warehouse.fact_sales
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;
"""

state_query = """
SELECT c.customer_state, SUM(f.price) AS revenue
FROM data_warehouse.fact_sales f
JOIN data_warehouse.dim_customer c ON f.customer_sk = c.customer_sk
WHERE c.customer_state IS NOT NULL
GROUP BY c.customer_state
ORDER BY revenue DESC
LIMIT 10;
"""

with engine.connect() as conn:
    rev    = pd.read_sql(rev_query,   conn)
    cat    = pd.read_sql(cat_query,   conn)
    scores = pd.read_sql(score_query, conn)
    states = pd.read_sql(state_query, conn)

# Label "Jan 2017", "Feb 2017" …
rev["label"] = rev["month_name"].str.strip() + " " + rev["year"].astype(str)

# ── Layout ─────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted")
fig = plt.figure(figsize=(18, 12))
fig.suptitle("Data Warehouse Dashboard", fontsize=20, fontweight="bold", y=0.98)
gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

ax1 = fig.add_subplot(gs[0, :])
ax2 = fig.add_subplot(gs[1, 0])
ax3 = fig.add_subplot(gs[1, 1])

# 1 ── Revenue per month
sns.lineplot(data=rev, x=range(len(rev)), y="revenue", marker="o", linewidth=2, ax=ax1)
ax1.set_title("Revenue Mensuelle (fact_sales)", fontsize=14)
ax1.set_xlabel("")
ax1.set_ylabel("Revenue")
step = max(1, len(rev) // 12)
ax1.set_xticks(range(0, len(rev), step))
ax1.set_xticklabels(rev["label"].iloc[::step], rotation=45, ha="right", fontsize=8)

# 2 ── Top 10 categories
cat_sorted = cat.sort_values("revenue")
sns.barplot(data=cat_sorted, x="revenue", y="category", ax=ax2, palette="Blues_d")
ax2.set_title("Top 10 Categories ", fontsize=14)
ax2.set_xlabel("Revenue")
ax2.set_ylabel("")

# 3 ── Review score distribution
ax3.bar(scores["review_score"], scores["cnt"],
        color=sns.color_palette("muted", len(scores)), edgecolor="white")
ax3.set_title("Review Score Distribution", fontsize=14)
ax3.set_xlabel("Score")
ax3.set_ylabel("Number of Orders")
ax3.set_xticks([1, 2, 3, 4, 5])

plt.savefig("dashboard.png", dpi=150, bbox_inches="tight")
print("Saved → dashboard.png")
plt.show()
