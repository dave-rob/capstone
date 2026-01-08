from classes.IngestRawData import IngestRawData

job =  IngestRawData(
        name="Ingest_BQ_Standards",
        csv="BQStandards",
        drop=["2013BQ"]
    )

job.execute()
