"""
This file is used to interact with S3. In order to use it, standard credentials and configuration files
will need to be set up. You can follow the below resources on how to set it up.

resources:

1. https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
2. https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
"""

__AUTHORS__ = "Terrill, Nebiyu, Estephanos"
__status__ = "Development"

import logging
import boto3
import tempfile
import os
import polars as pl


class S3IO():
    """
    Wrapper class to the boto3 package for easy interaction with S3. Premise is to
    give data scientists and data engineers tools to enhance their workflows
    """
    # initialization
    def __init__(self,
                 bucket: str,
                 profile: str = 'default'):
        """
        Initialization of the S3IO object.

        Parameters
        ----------
        bucket: str
            The name of the AWS bucket

        profile: str
            The name of the profile that holds the access key
            and access id in the AWS credentials file
        """
        self.bucket = bucket
        self._profile = profile
        # establish a connection with s3
        logging.info("Establishing a connection with S3 using passed parameters")
        try:
            session = boto3.Session(profile_name=self._profile)
        except Exception as e:
            logging.error(f"{e}\nCheck spelling of profile or properly set it in credentials file")
            raise ValueError
        self.s3_client = session.client('s3')
        self.s3_resource = session.resource('s3')


    def s3_is_dir(self,
                  path: str) -> bool:
        """
        Function checks if a given path exists in s3

        Parameters
        ----------
        path: str
            s3 path being verified

        Returns
        -------
        bool:   True if the path exists and False if not
        """
        objs = list(self.s3_resource.Bucket(
            self.bucket).objects.filter(Prefix=path))
        return len(objs) > 1 or (len(objs) == 1 and objs[0].key != path)

    def s3_list(self,
                path: str) -> list:
        """
        Function returns the list of objects in a given path for the
        bucket that was initialized.

        Parameters
        ----------
        path: str
            The path where objects are stored

        Returns
        -------
            list:   the list of objects that exist in the path that was passed.
                    If the path is a directory that does not exist in the bucket,
                    the function will return an empty list.
        """
        # Retrieve the objects in the passed path
        all_obj = self.s3_resource.Bucket(self.bucket).objects.filter(Prefix=path)
        # Parse dictionary to retrieve the objects
        result = [x.key for x in all_obj]
        # Check if the reusults is an empty list
        if not result:
            logging.warning((f"Results produced an empty list for path: {path} "
                             f"Check the spelling of the path or its existence"))
        return result

    def s3_read_parquet(self,
                        file_path: str) -> pl.DataFrame:
        """
        Function takes the full path of a csv file as input and outputs
        a pandas dataframe of the file.

        Parameters
        ----------
        file_path: str
            The full path of the file, example -> file/located/here.csv

        Returns
        -------
        pd.DataFrame:   Dataframe of the file content
        """
        # Get object from s3
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
        data = pl.read_parquet(obj['Body'])
        return data

    def s3_write_parquet(self,
                         df: pl.DataFrame,
                         file_path: str) -> None:
        """
        Function takes a local pandas dataframe and writes it to a specified
        path as a csv file

        Parameters
        ----------
        df: pd.DataFrame
            local pandas dataframe

        file_path: str
            the full file path where the dataframe will be saved in s3
            example ->  file/located/here.csv
        """
        # Check if the path for the file exists
        # split_path = file_path.split("/")[:-1]
        # path = "/".join(split_path)
        # if not self.s3_is_dir(path):
        #     logging.error(f"The given path does not exist: {path}")
        #     raise ValueError("Please pass a valid path")
        # Create a temp file locally using df and load to the path s3
        with tempfile.NamedTemporaryFile(delete=True, mode='r+') as temp:
            df.write_parquet(f"{temp.name}.parq")
            self.s3_resource.Bucket(self.bucket).upload_file(
                f"{temp.name}.parq", Key=file_path)