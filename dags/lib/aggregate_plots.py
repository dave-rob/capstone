
def year_plot():
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

def plot_age_group():
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import os


    df = pd.read_parquet("data/aggregates/AgeGroupQual")
    os.makedirs("data/plots", exist_ok=True)

    pivot_df = (
        df.pivot(
            index="Age Group",
            columns="Gender",
            values="qualified_runners"
        )
        .fillna(0)
        .sort_index()
    )

    age_order = ["Under 20", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80 and Over"]

    pivot_df = pivot_df.reindex(
        [a for a in age_order if a in pivot_df.index]
    )

    x = np.arange(len(pivot_df.index)) * 1.5
    width = 0.35

    plt.figure(figsize=(15, 10))

    plt.bar(x - width, pivot_df["M"], width, label="Male")
    plt.bar(x,         pivot_df["F"], width, label="Female")
    plt.bar(x + width, pivot_df["X"], width, label="Non-binary")

    plt.xticks(x, pivot_df.index, rotation=45)
    plt.xlabel("Age Group")
    plt.ylabel("Qualified Runners")
    plt.title("Boston Qualifiers by Age Group and Gender")
    plt.legend()

    plt.tight_layout()
    plt.savefig("data/plots/bq_by_age_group_gender.png")
    plt.close()