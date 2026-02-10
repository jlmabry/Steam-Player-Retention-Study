import pandas as pd
from sklearn.metrics import r2_score
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

df = pd.read_excel('game_dataset_combined.xlsx')

# Select tag columns + playtime

tag_cols = [c for c in df.columns if c.startswith("tags.")]
X = df[tag_cols + ['average_playtime_forever']].fillna(0)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

from sklearn.cluster import KMeans

# Try 5 clusters
kmeans = KMeans(n_clusters=5, random_state=42)
df["cluster"] = kmeans.fit_predict(X_scaled)

# Evaluate clustering with silhouette score
from sklearn.metrics import classification_report, mean_squared_error, silhouette_score
score = silhouette_score(X_scaled, df["cluster"])
print("Silhouette Score for 5 clusters:", score)

cluster_summary = df.groupby("cluster")[tag_cols + ['average_playtime_forever']].mean()
print(cluster_summary)

# visualization with PCA
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

pca = PCA(n_components=2)
coords = pca.fit_transform(X_scaled)

plt.scatter(coords[:,0], coords[:,1], c=df["cluster"], cmap='tab10')
plt.xlabel("PCA1")
plt.ylabel("PCA2")
plt.title("Game Clusters by Tags + Playtime")
plt.show()

# --- Convert verdicts to numeric if not already done ---
df["verdict_numeric"] = df["verdict"].map({"Recommended": 1, "Not Recommended": 0})

# --- Feature set: tags only ---
tag_cols = [c for c in df.columns if c.startswith("tags.")]
X = df[tag_cols].fillna(0)

# --- Targets ---
y_logistic = df["verdict_numeric"]       # binary target for logistic regression
y_linear = df["average_playtime_forever"]            # continuous target for linear regression

# --- Train/test splits ---
X_train_log, X_test_log, y_train_log, y_test_log = train_test_split(
    X, y_logistic, test_size=0.2, random_state=42
)

X_train_lin, X_test_lin, y_train_lin, y_test_lin = train_test_split(
    X, y_linear, test_size=0.2, random_state=42
)
# --- Logistic Regression: Predict positive reviews ---
log_model = LogisticRegression(max_iter=1000)
log_model.fit(X_train_log, y_train_log)

y_pred_log = log_model.predict(X_test_log)
print("Logistic Regression Results:")
print(classification_report(y_test_log, y_pred_log))

# --- Visualize top logistic coefficients ---
coef = pd.Series(log_model.coef_[0], index=X.columns)
top_coef = coef.abs().sort_values(ascending=False).head(20)

plt.figure(figsize=(10,6))
top_coef.plot(kind="barh", color="purple")
plt.title("Top Logistic Regression Coefficients (Review Prediction)")
plt.xlabel("Coefficient Magnitude")
plt.gca().invert_yaxis()
plt.show()

# --- Linear Regression: Predict playtime ---
lin_model = LinearRegression()
lin_model.fit(X_train_lin, y_train_lin)

y_pred_lin = lin_model.predict(X_test_lin)
print("\nLinear Regression Results:")
print("MSE:", mean_squared_error(y_test_lin, y_pred_lin))
print("R²:", r2_score(y_test_lin, y_pred_lin))

# --- Visualize predicted vs actual playtime ---
plt.figure(figsize=(8,6))
plt.scatter(y_test_lin, y_pred_lin, alpha=0.3, color="purple")
plt.plot([y_test_lin.min(), y_test_lin.max()],
         [y_test_lin.min(), y_test_lin.max()],
         "k--", lw=2)
plt.xlabel("Actual Playtime")
plt.ylabel("Predicted Playtime")
plt.title("Linear Regression: Predicted vs Actual Playtime")
plt.show()
