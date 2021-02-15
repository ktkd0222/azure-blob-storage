#!/usr/bin/python
# -*- coding: utf-8 -*-

# AzureBlobFileUploader.py
import os
import glob
from logging import getLogger, NullHandler, DEBUG, INFO, WARNING
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceNotFoundError
)

class AzureBlobFileUploader():
  def __init__(self, connection_string, container_name, target_folder_path, source_folder_path):
    # Logger
    self._logger = getLogger(__name__)
    self._logger.addHandler(NullHandler())
    self._logger.setLevel(DEBUG)
    self._logger.propagate = True
    # Azure Blob / Urllib3のログが大量に出力されるのでWARNING以上に設定
    getLogger("azure").setLevel(WARNING)
    getLogger("urllib3").setLevel(WARNING)
    
    # Initialize the connection to Azure storage account
    self.blob_service_client =  BlobServiceClient.from_connection_string(connection_string)
    self.container_name = container_name
    self.target_folder = target_folder_path
    self.source_folder = source_folder_path

    self._logger.info(f'Initialize AzureBlobFileUploader. Container Name:{container_name}')

  # upload_local_file
  # 指定したローカルファイルをBlob上にアップロードします
  def upload_local_file(self):
      try:
          files = glob.glob(os.path.join(self.source_folder, '*'))
          # local file read
          for file in files:
            # self._logger.info(f'Delete blob file :{os.path.join(self.target_folder, os.path.basename(file))}')
            # blob_client.delete_blob()
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=os.path.join(self.target_folder, os.path.basename(file)))

            self._logger.info(f'Upload local file :{os.path.join(self.target_folder, os.path.basename(file))}')
            with open(file, "rb") as data:
                blob_client.upload_blob(data)

      except ResourceExistsError as ex:
          self._logger.error(f"[ResourceExistsError][{self.__module__}]:{ex.message}")
      except ResourceNotFoundError as ex:
          self._logger.error(f"[ResourceNotFoundError][{self.__module__}]:{ex.message}")
      except Exception as ex:
          self._logger.error(f"[ExceptionError][{self.__module__}]:{ex.message}")
