import os
import pandas as pd
from mlProject import logger
from mlProject.entity.config_entity import DataValidationConfig

class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def validate_all_columns(self) -> bool:
        try:
            data = pd.read_csv(self.config.data_file)  # ✅ Use correct reference
            all_cols = set(data.columns)
            all_schema = set(self.config.all_schema.keys())

            # ✅ Ensure all required columns exist
            missing_cols = all_schema - all_cols
            validation_status = len(missing_cols) == 0

            # ✅ Write only the final validation status to file
            with open(self.config.STATUS_FILE, 'w') as f:
                f.write(f"Validation status: {validation_status}")

            if not validation_status:
                logger.warning(f"Missing columns: {missing_cols}")
            else:
                logger.info("All columns are present!")

            return validation_status

        except Exception as e:
            logger.error(f"Error in Data Validation: {e}")
            raise e  # ✅ Correct exception handling
