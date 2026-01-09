from classes.IngestRawData import IngestRawData

job =  IngestRawData(
        name="Ingest_Races",
        csv="Races",
        drop=["Country", "Continent", "Type", "Include", "AgeBand", "Net", "Adjustment"]
    )

job.execute()