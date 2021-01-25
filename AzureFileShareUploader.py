#!/usr/bin/python
# -*- coding: utf-8 -*-

# AzureFileShareUploader.py
# Azure File Shareへのファイルアップロード処理を実装したクラス
import os
import constants.constants as cst
import hashlib
import time

from azureblob.HashCalculate import HashCalculate
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from logging import getLogger, NullHandler, DEBUG, INFO, WARNING
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceNotFoundError
)
from azure.storage.fileshare import (
    ShareServiceClient,
    ShareClient,
    ShareDirectoryClient,
    ShareFileClient
)


class AzureFileShareUploader():
  def __init__(self):
    # Logger
    self._logger = getLogger(__name__)
    self._logger.addHandler(NullHandler())
    self._logger.setLevel(DEBUG)
    self._logger.propagate = True
    # Azure Blob / Urllib3のログが大量に出力されるのでWARNING以上に設定
    getLogger("azure").setLevel(WARNING)
    getLogger("urllib3").setLevel(WARNING)

    # Initialize the connection to Azure storage account with sas token
    # [注意]
    # SAS Tokenは有効期限があるので切れた場合は接続文字列（SAS Token URL）を更新する必要があります
    self.share_client = ShareClient.from_connection_string(cst.const.FILE_SHARE_CONNECTION_STRING, cst.const.FILE_SHARE_NAME)
    self._logger.info(f'Initialize AzureFileShareUploader.')

  # list_files_and_dirs
  # 指定した対象フォルダの中身を表示します
  # ※今回は未使用
  # [引数]
  # - file_name : CSVファイル名称
  # - file_content : ファイル内容（byte[]）
  def list_files_and_dirs(self):
      try:
          for item in list(self.share_client.list_directories_and_files(cst.const.UPLOAD_FILE_PATH)):
              if item["is_directory"]:
                  print("Directory:", item["name"])
              else:
                  print("File:", cst.const.UPLOAD_FILE_PATH + "/" + item["name"])
      except ResourceNotFoundError as ex:
          raise Exception(f"[ResourceNotFoundError][{self.__module__}]:{ex.message}")
      except Exception as ex:
          raise Exception(f"[ExceptionError][{self.__module__}]:{ex.message}")

  # upload_local_file
  # 指定したローカルファイルをAzure共有フォルダ上にアップロードします
  # ※今回は未使用
  # [引数]
  # - file_name : CSVファイル名称
  # - file_content : ファイル内容（byte[]）
  def upload_local_file(self):
      try:
          upload_file = os.path.join(cst.const.NOTICE_HTML_TEMPLATE_FOLDER_PATH, cst.const.NOTICE_HTML_OUTPUT_FILE_NAME)
          with open(upload_file, "rb") as file:
              data = file.read()
          dir_client = self.share_client.get_directory_client(cst.const.UPLOAD_FILE_PATH)
          self._logger.info(f"Uploading to:{os.path.join(cst.const.FILE_SHARE_NAME, cst.const.UPLOAD_FILE_PATH)}")
          dir_client.upload_file(cst.const.NOTICE_HTML_OUTPUT_FILE_NAME, data)
      
      except ResourceExistsError as ex:
          raise Exception(f"[ResourceExistsError][{self.__module__}]:{ex.message}")
      except ResourceNotFoundError as ex:
          raise Exception(f"[ResourceNotFoundError][{self.__module__}]:{ex.message}")
      except Exception as ex:
          raise Exception(f"[ExceptionError][{self.__module__}]:{ex.message}")
