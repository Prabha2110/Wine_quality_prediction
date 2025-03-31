from mlProject.config.configuration import ConfigurationManager
from mlProject.components.DATA_INGESTION import DataIngestion
from mlProject import logger


STAGE_NAME = "Data Ingestion stage"

class DataIngestionTrainingPipeline:
    def __init__(self):
        pass

    def main(self):
        config = ConfigurationManager()
        data_ingestion_config = config.get_data_ingestion_config()
        data_ingestion = DataIngestion(config=data_ingestion_config)

        # Download the file
        data_ingestion.download_file()

        # Only extract if it's a ZIP file
        if str(data_ingestion_config.local_data_file).endswith(".zip"):
            data_ingestion.extract_zip_file()
        else:
            logger.info("Skipping extraction as the downloaded file is not a ZIP.")
