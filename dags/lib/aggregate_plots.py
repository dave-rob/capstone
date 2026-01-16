
def year_plot():
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    import os


    df = pd.read_parquet("/opt/airflow/data/aggregates/YearQual")
    df["Year"] = df['Year'].astype('str')
    os.makedirs("/opt/airflow/artifacts/plots", exist_ok=True)

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
    plt.savefig("/opt/airflow/artifacts/plots/bq_by_year.png")
    plt.close()

def plot_age_group():
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import os


    df = pd.read_parquet("/opt/airflow/data/aggregates/AgeGroupQual")
    os.makedirs("/opt/airflow/artifacts/plots", exist_ok=True)

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
    plt.savefig("/opt/airflow/artifacts/plots/bq_by_age_group_gender.png")
    plt.close()

def race_plot():
    import pandas as pd
    import matplotlib.pyplot as plt
    import os

    df = pd.read_parquet("/opt/airflow/data/aggregates/RaceQual")

    big_marathons = df[df['total_runners'] > 10000]
    
    big_marathons["qualification_rate"] = big_marathons['qualified_runners']/big_marathons['total_runners']

    big_marathons_no_boston = big_marathons[big_marathons['Race'] != "Boston Marathon"]

    df = big_marathons_no_boston.sort_values("qualification_rate", ascending=True)
    os.makedirs("/opt/airflow/artifacts/plots", exist_ok=True)

    plt.figure(figsize=(10, 6))
    plt.barh(df["Race"], df["qualification_rate"])

    plt.xlabel("Qualification Rate")
    plt.ylabel("Race")
    plt.title("Boston Qualification Rate by Race")

    plt.tight_layout()
    plt.savefig("/opt/airflow/artifacts/plots/bq_rate_by_race.png")
    plt.close()

def plot_multiple_char():
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import os

    df = pd.read_parquet("/opt/airflow/data/aggregates/RaceCharQual")
    print(df)
    # Filter small groups
    df = df[df["total_runners"] >= 100]
    df["qualification_rate"] = df["qualified_runners"]/df["total_runners"]
    races = sorted(df["Race"].unique())
    genders = sorted(df["Gender"].unique())
    age_groups = sorted(df["Age Group"].unique())

    os.makedirs("/opt/airflow/artifacts/plots", exist_ok=True)

    n_races = len(races)
    fig, axes = plt.subplots(
        n_races, 1,
        figsize=(14, 4 * n_races),
        sharex=False
    )

    if n_races == 1:
        axes = [axes]

    bar_width = 0.35
    x = np.arange(len(age_groups))

    for ax, race in zip(axes, races):
        race_df = df[df["Race"] == race]

        for i, gender in enumerate(genders):
            gender_df = race_df[race_df["Gender"] == gender]

            rates = [
                gender_df[gender_df["Age Group"] == ag]["qualification_rate"].mean()
                if ag in gender_df["Age Group"].values else 0
                for ag in age_groups
            ]

            ax.bar(
                x + i * bar_width,
                rates,
                width=bar_width,
                label=gender
            )

        ax.set_title(f"{race} — Qualification Rate by Age Group & Gender")
        ax.set_xticks(x + bar_width / 2)
        ax.set_xticklabels(age_groups, rotation=45)
        ax.set_ylabel("Qualification Rate")
        ax.legend()

    axes[-1].set_xticks(x + bar_width / 2)
    axes[-1].set_xticklabels(age_groups, rotation=45)

    plt.tight_layout()
    plt.savefig("/opt/airflow/artifacts/plots/bq_rate_race_char.png")
    plt.close()