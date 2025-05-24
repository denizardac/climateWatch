"""
HDFS utility functions for ClimateWatch project.
"""
import os
import io
from typing import Union, List, Optional
from hdfs import InsecureClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class HDFSManager:
    def __init__(self, hdfs_url: str = None, hdfs_user: str = None):
        """
        Initialize HDFS client connection.
        
        Args:
            hdfs_url: HDFS namenode URL (e.g., 'http://localhost:9870')
            hdfs_user: HDFS username
        """
        self.hdfs_url = hdfs_url or os.getenv('HDFS_URL', 'http://localhost:9870')
        self.hdfs_user = 'root'
        
        # WebHDFS API endpoint yapılandırması
        self.client = InsecureClient(
            url=self.hdfs_url,
            user=self.hdfs_user,
            timeout=100
        )

    def write_df_to_parquet(self, df: pd.DataFrame, hdfs_path: str) -> None:
        """
        Write a pandas DataFrame to HDFS in Parquet format.
        
        Args:
            df: Pandas DataFrame to write
            hdfs_path: Target path in HDFS
        """
        try:
            # Convert DataFrame to Parquet bytes
            table = pa.Table.from_pandas(df)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            
            # Write bytes to HDFS
            with self.client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
                writer.write(buf.getvalue())
                
        except Exception as e:
            print(f"Error writing to HDFS: {str(e)}")
            raise

    def read_parquet_to_df(self, hdfs_path: str) -> pd.DataFrame:
        """
        Read a Parquet file from HDFS into a pandas DataFrame.
        
        Args:
            hdfs_path: Path to Parquet file in HDFS
            
        Returns:
            pandas DataFrame
        """
        try:
            with self.client.read(hdfs_path, encoding='utf-8') as reader:
                buf = io.BytesIO(reader.read())
                buf.seek(0)
                table = pq.read_table(buf)
                return table.to_pandas()
        except Exception as e:
            print(f"Error reading from HDFS: {str(e)}")
            raise

    def list_directory(self, hdfs_path: str = '/') -> List[str]:
        """
        List contents of a directory in HDFS.
        
        Args:
            hdfs_path: Directory path in HDFS
            
        Returns:
            List of files and directories
        """
        try:
            return self.client.list(hdfs_path)
        except Exception as e:
            print(f"Error listing HDFS directory: {str(e)}")
            return []

    def upload_file(self, local_path: str, hdfs_path: str) -> None:
        """
        Upload a local file to HDFS.
        
        Args:
            local_path: Path to local file
            hdfs_path: Target path in HDFS
        """
        try:
            self.client.upload(hdfs_path, local_path)
        except Exception as e:
            print(f"Error uploading to HDFS: {str(e)}")
            raise

    def download_file(self, hdfs_path: str, local_path: str) -> None:
        """
        Download a file from HDFS to local filesystem.
        
        Args:
            hdfs_path: Source path in HDFS
            local_path: Target path in local filesystem
        """
        try:
            self.client.download(hdfs_path, local_path)
        except Exception as e:
            print(f"Error downloading from HDFS: {str(e)}")
            raise

    def delete(self, hdfs_path: str, recursive: bool = False) -> None:
        """
        Delete a file or directory from HDFS.
        
        Args:
            hdfs_path: Path to delete in HDFS
            recursive: If True, recursively delete directories
        """
        try:
            self.client.delete(hdfs_path, recursive=recursive)
        except Exception as e:
            print(f"Error deleting from HDFS: {str(e)}")
            raise

    def mkdir(self, hdfs_path: str) -> None:
        """
        Create a directory in HDFS.
        
        Args:
            hdfs_path: Path to create in HDFS
        """
        try:
            self.client.makedirs(hdfs_path)
        except Exception as e:
            print(f"Error creating directory in HDFS: {str(e)}")
            raise 