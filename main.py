#!/usr/bin/python
# -*- coding: utf-8 -*-

from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO
from AzureBlobFileDownloader import AzureBlobFileDownloader
from AzureBlobFileUploader import AzureBlobFileUploader

# 定数定義
connection_string:str = r'DefaultEndpointsProtocol=https;AccountName=devblob20210127;AccountKey=2K3g3+iylnd2F8JhmWj+5K2pW8ttsfpbgigltu00tdBc1Bb1mSjosKkfZ8ni6Vx+bSjAcmuJa44FZK2EJ0qNgA==;EndpointSuffix=core.windows.net;'
container_name:str = r'storage'
download_path:str = r'input'
upload_path:str = r'output'


def main():
    # Initialize Root Logger
    logger = initializeLogger()
    logger.info(f'Initialize Logger finished.')
    
    azure_blob_file_downloader = AzureBlobFileDownloader(connection_string, container_name, download_path)
    azure_blob_file_downloader.download_blob()
    azure_blob_file_uploader = AzureBlobFileUploader(connection_string, container_name, upload_path, download_path)
    azure_blob_file_uploader.upload_local_file()


def initializeLogger():
    # Logger
    logger = getLogger()

    # Initalize Logger
    formatter = Formatter('[%(asctime)s][%(name)s][%(funcName)s][%(levelname)s] : %(message)s')
    # ログレベルを DEBUG に変更
    handler = StreamHandler()
    handler.setLevel(DEBUG)
    handler.setFormatter(formatter)
    logger.setLevel(DEBUG)
    logger.addHandler(handler)

    return logger

if __name__ == '__main__':
    main()
