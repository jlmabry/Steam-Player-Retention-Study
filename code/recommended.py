import os
import pandas as pd

# Load master data
metadata = pd.read_excel("gameMetaData.xlsx")
metadata.columns = [c.lower() for c in metadata.columns] # normalize column names

# Folder with review files
review_folder = "cleaned_csvs"

# Prepare results
results = []

# Get list of review files
files = [f for f in os.listdir(review_folder) if f.endswith('.csv')]
total_files = len(files)

for i, fname in enumerate(files, 1):
    appid = fname.split("_")[0]
    review_path = os.path.join(review_folder, fname)

    try:
        df = pd.read_csv(review_path, encoding='utf-8', on_bad_lines='skip')
        if "recommend" not in df.columns:
            continue
        #Normalize recommend column
        df['recommend'] = df['recommend'].astype(str).str.strip().str.lower()

        total = len(df)
        recommended = df['recommend'].str.lower().eq("recommended").sum()
        not_recommended = df['recommend'].str.lower().eq("not recommended").sum()

        percent_recommended = round((recommended / total) * 100, 2) if total > 0 else 0
        verdict = "Recommended" if percent_recommended > not_recommended else "Not Recommended"
        
        # Lookup game title
        game_info = metadata[metadata['appid'] == int(appid)]
        title = game_info.iloc[0]["title"] if not game_info.empty else "Unknown"

        results.append({
            "appid": appid,
            "title": title,
            "total_reviews": total,
            "recommended": recommended,
            "not_recommended": not_recommended,
            "percent_recommended": percent_recommended,
            "verdict": verdict
        })

        print(f"[{i}/{total_files}] Processed {title} ({appid}) - {verdict} ({percent_recommended}%)")
    except Exception as e:
        print(f"[{i}/{total_files}] Error processing {fname}: {e}")

# Convert to Dataframe
summary_df = pd.DataFrame(results)

# Show results
print("\nSummary")
print(summary_df.sort_values(by="total_reviews", ascending=False).to_string(index=False))
summary_df.to_excel("recommendation_summary.xlsx", index=False)
    