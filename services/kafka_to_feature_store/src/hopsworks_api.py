import hopsworks
from src.config import config
import pandas as pd
def push_data_to_feature_store(feature_group_name: str,
        feature_group_version: int,
        data: dict) -> None:
        """
        Pushes OHLC data to the feature store, writing it to a feature group
        with a specific name and version
        
        Args:
            feature_group_name(str): name of the feature group to write to
            feature_group_version(int): version of the feature group to write to
            data(dict): OHLC data to push to the feature store
        
        Returns:
            None
        """

        #Authenticate with Hopsworks API
        project = hopsworks.login(
                project = config.hopsworks_project_name,
                api_key_value = config.hopsworks_api_key
        )

        #Get feature store
        feature_store = project.get_feature_store()

        # Get or create the feaute group, we will be saving feature data to
        ohlc_feature_group = feature_store.get_or_create_feature_group(
                name = feature_group_name,
                version = feature_group_version,
                description = "OHLC data coming from kafka",
                primary_key = ["product_id", "timestamp"],
                event_time = "timestamp",
                online_enabled = True 
        )

        data = pd.DataFrame([data])

        ohlc_feature_group.insert(data)