#!/usr/bin/python
# -*- coding: utf-8 -*-

# AzureBlobFileDownloader.py
import os
from logging import getLogger, NullHandler, DEBUG, INFO, WARNING
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient

class AzureBlobFileDownloader():
  def __init__(self, connection_string, container_name, folder_path):
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
    self.my_container = self.blob_service_client.get_container_client(container_name)
    self.target_folder = folder_path

    self._logger.info(f'Initialize AzureBlobFileDownloader. Container Name:{container_name}')

  # download_blob
  # ダウンロードしたBlobファイルをローカルに保存します
  # [引数]
  # - file_name : ファイル名称
  # - file_content : ファイル内容（byte[]）
  def download_blob(self):
    my_blobs = self.my_container.list_blobs()
    self._logger.info(f'Download target blob : {os.path.join(self.target_folder, "*")}')
    for blob in my_blobs:
        if blob.name.startswith(self.target_folder) :
            dirname, filename = os.path.split(blob.name)
            bytes = self.my_container.get_blob_client(blob).download_blob().readall()
            self.save_blob(blob.name, bytes)
        else:
            self._logger.debug(f'Ignore blob : {blob.name}')

  # save_blob
  # ダウンロードしたBlobファイルをローカルに保存します（既にファイルが存在した場合は上書処理）
  # [引数]
  # - file_name : ファイル名称
  # - file_content : ファイル内容（byte[]）
  def save_blob(self, file_name, file_content):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
 
    with open(file_name, "wb") as file:
      file.write(file_content)