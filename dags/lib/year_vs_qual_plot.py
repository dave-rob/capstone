
def plot():
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    import os


    df = pd.read_parquet("data/aggregates/YearQual")
    df["Year"] = df['Year'].astype('str')
    os.makedirs("data/plots", exist_ok=True)

    colors = [
    "tab:orange" if entry else "tab:blue"
    for entry in df["2026_BQ_Entry"]
    ]

    legend_elements = [
    Patch(facecolor="tab:blue", label="2020 BQ Standard"),
    Patch(facecolor="tab:orange", label="2026 BQ Standard"),
    ]


    plt.figure()
    plt.bar(df["Year"], df["qualified_runners"], color=colors)
    plt.title("Boston Qualifiers by Year")
    plt.xlabel("Year")
    plt.ylabel("Qualified Runners")
    plt.legend(handles=legend_elements)
    plt.tight_layout()
    plt.savefig("data/plots/bq_by_year.png")
    plt.close()