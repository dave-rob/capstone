def createModel():
    import pandas as pd
    import statsmodels.api as sm
    import statsmodels.formula.api as smf
    import os

    os.makedirs("/opt/airflow/artifacts", exist_ok=True)

    df = pd.read_parquet("/opt/airflow/data/aggregates/RaceCharQual")
    print(df)

    # df = df[df["total_runners"] > 50]
    print(df)

    df["not_qualified_runners"] = df["total_runners"] - df["qualified_runners"]

    df = df.rename(columns={
        "2026_BQ_Entry": "BQ_2026_Entry",
        "Age Group": "Age_Group"
    })

    #use GLM, Y isnt normally distributed
    model = smf.glm(
        formula="""
            qualified_runners + not_qualified_runners
            ~ Year + Gender + Age_Group + BQ_2026_Entry
        """,
        data=df,
        family=sm.families.Binomial()
    ).fit()

    print(model.summary())

    with open("/opt/airflow/artifacts/RegressionModelSummary.txt", 'w') as file:
        file.write(model.summary().as_text())