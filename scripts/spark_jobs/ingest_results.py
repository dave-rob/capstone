from classes.IngestRawData import IngestRawData

job =  IngestRawData(
        name="Ingest_Results",
        csv="Results",
        drop=["Name", "Zip"]
    )

job.execute()