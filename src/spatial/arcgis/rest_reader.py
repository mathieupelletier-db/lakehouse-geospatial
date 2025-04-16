from pathlib import Path
from pyspark.sql import SparkSession
from typing import Generator, Optional, Tuple, Dict
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests
import json

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import *
except ImportError as e:
    print(f"Error importing PySpark custom data sources: {e}. This feature is only available in Databricks Runtime 15.2 and above.")
    print("PySpark custom data sources are in Public Preview in Databricks Runtime 15.2 and above, and on serverless environment version 2. Streaming support is available in Databricks Runtime 15.3 and above.")
    raise

class ArcgisFeatureServerToGeometryDataSourceReader(DataSourceReader):
    """
    Data source reader to read ArcGIS features from the REST API and convert them to a dataframe.

    Attributes:
        schema (StructType): The schema of the output data.
        options (dict): Configuration options to customize the data reader.

    Options:
        - `url` (str): The ArcGIS FeatureServer endpoint without query parameters. **Required**.
        - `where` (str): Filter results based on the where clause. Defaults to 1=1.
        - `outFields` (str): Determine which field to includes in the response. Defaults to *.
        - `chunkSize` (int): Process records in chunks of `chunkSize` records at a time. Defaults to 1000 which is the maximum supported by ArcGIS server.

    Example Usage:
        df = (
            spark.read.format("arcgis")
            .option("url", url)
            .option("where", "1=1")
            .option("outFields", "*")
            .option("chunkSize", "1000")
            .load()
        )
    """
    def __init__(self, schema: StructType, options: dict):
        """
        Initialize the ArcgisFeatureServerToGeometryDataSourceReader.

        Args:
            schema (StructType): The schema of the output data.
            options (dict): Options to configure the data reader, such as file path and filters.
        """
        self.schema: StructType = schema
        self.options: dict = options

    def read(self, partition: Optional[int] = None):
        """
        Call the REST API and yield data for each records.

        Args:
            partition (Optional[int]): Partition index, if applicable. Not implemented.

        Yields:
            tuple: A tuple containing the feature
        """

        # TODO improve performance using partitioning: https://github.com/hubert-dudek/medium/blob/main/own-data-sources-partitions/myrest/reader.py

        # Extract options
        url: str = self.options.get("url")
        if not url:
            raise ValueError("The 'url' option is required.")

        whereClause: str = self.options.get("where", "1=1")
        outFields: str = self.options.get("outFields", "*")
        resultRecordCount: int = int(self.options.get("chunkSize", 1000))
        if resultRecordCount > 1000:
            resultRecordCount = resultRecordCount

        # make sure url ends with query
        resultOffset = 0
        condition = True

        while condition:
            new_url = f'{url}/query?where={whereClause}&resultOffset={resultOffset}&resultRecordCount={resultRecordCount}&outFields={outFields}&returnExceededLimitFeatures=true&f=pgeojson'
            response = requests.get(new_url)
            data = response.json()

            if 'features' in data:
                for feature in data['features']:
                    tags: Dict[str, str] = {}
                    # Extract tags for the feature
                    tags = {k: v for k, v in feature['properties'].items()}
                    
                    yield (feature['id'], feature['type'], json.dumps(feature['geometry']), tags)

            if 'properties' not in data or not data['properties']['exceededTransferLimit']:
                condition = False

            resultOffset += int(resultRecordCount)

class ArcgisFeatureServerToGeometryDataSource(DataSource):
    """
    A custom data source to convert ArcGIS features to geometries in GeoJSON format.
    """
    @classmethod
    def name(cls) -> str:
        """
        Get the name of the data source.

        Returns:
            str: The name of the data source.
        """
        return "arcgis"

    def schema(self):
        """
        Define the schema for the output data.

        Returns:
            StructType: The schema including fields for ID, type, geometry, and tags.
        """
        return StructType([
            StructField("id", LongType(), True),
            StructField("type", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("properties", MapType(StringType(), StringType()), True)
        ])


    def reader(self, schema: StructType) -> ArcgisFeatureServerToGeometryDataSourceReader:
        """
        Create a data source reader for retrieving geometries from an ArcGIS Feature Server through the REST API.

        Args:
            schema (StructType): The schema of the output data.

        Returns:
            ArcgisFeatureServerToGeometryDataSourceReader: An instance of the data source reader.
        """
        return ArcgisFeatureServerToGeometryDataSourceReader(schema, self.options)

def register_arcgis_data_source():
    if os.getenv("IS_SERVERLESS") == "TRUE":
        raise RuntimeError(
            "Error: This data source can only be executed in a non-serverless context. "
            "Please attach the notebook to a traditional compute cluster and try again."
        )
    
    spark = SparkSession.getActiveSession()
    try:
        spark.dataSource.register(ArcgisFeatureServerToGeometryDataSource)
        print("Custom data source 'arcgis' registered successfully.")
    except AttributeError:
        print("Error registering custom data source: PySpark custom data sources are not supported in this environment.")
