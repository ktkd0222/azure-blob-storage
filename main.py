#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO
from azureblob.AzureBlobFileDownloader import AzureBlobFileDownloader
from azureblob.AzureFileShareUploader import AzureFileShareUploader
from vertica.VerticaClient import VerticaClient
from notice.NoticeClient import NoticeClient
import constants.constants as cst
import time

def main():
    # 通知用
    notice_client = NoticeClient()
    # Initialize Root Logger
    logger = initializeLogger()
    logger.info(f'Initialize Logger finished.')
    message_list = []
    is_notice = True
    try:
        # Start notice logging
        notice_client.logging_local_file(f'Marketwatch Trend (Food Trend) process start.')
        start = time.time()
        ### CSV File Download from Azure Blob
        logger.info(f'Azure blob download start.')
        azure_blob_file_downloader = AzureBlobFileDownloader()
        if azure_blob_file_downloader.file_check_target_month_blobs_in_container() :
            (file_number, file_names) = azure_blob_file_downloader.download_target_month_blobs_in_container()
            logger.info(f'Azure blob download finished. Total download file number:{file_number} / Files:{"/".join(file_names)}')
            ### Check File Nums  0 -> Nothing to do. 4 -> Next Process. Else -> warning message
            if file_number == 4 :
                ### Processing start Notification
                notice_client.create_success_notice(f'- Azure Blob上で全てのファイルを検知したので、Verticaへのファイルアップロード処理を開始します。')

                ### CSV File Upload to Vertica
                logger.info(f'Vertica upload start.')
                vertica_client = VerticaClient()
                update_results = vertica_client.csv_upload()
                update_message = ""
                for update_result in update_results:
                    update_message += f' - {update_result[1]}  \n`{update_result[0]}`  \n'
                logger.info(f'Vertica upload finished.')
                
                ### Notification Upload to Azure File Share
                logger.info(f'Notification upload start.')
                notice_client.create_tableau_notice_file()
                file_share_client = AzureFileShareUploader()
                file_share_client.upload_local_file()
                logger.info(f'Notification upload finished.')
                logger.info(f'Main Process All finished.')
                message_list.append(f' - CSVファイルからVerticaへのアップロード処理が完了しました。  \n下記の結果を確認してください。  \n{update_message}')
            elif file_number == 0 :
                # Start notice logging
                notice_client.logging_local_file(f'File Already Downloaded. Nothing to do.')
                message_list.append(f' - ファイルは全てダウンロードされており、Azure Blob上のファイルにも変更がありません。処理を終了します。')
                is_notice = False
            else :
                # Warning Message
                notice_client.logging_local_file(f'File Already Downloaded. But File Change Detect. file number:{file_number} / Files:{"/".join(file_names)}')
                message_list.append(f' - ファイルダウンロード処理が完了しましたが、一部Azure Blob上のファイルに変更が入っています。問題無ければこのメッセージは無視してください。 `file number:{file_number} / Files:{"/".join(file_names)}`')
                # if 1-3 or 5 over file exists remove local file
                # azure_blob_file_downloader.delete_local_file()
        else:
            logger.info(f'Target month files not detect.')
            message_list.append(f' - Azure Blob上で対象月のデータを確認出来ませんでした。Azure Data Factoryが動作対象日を経過していない可能性があります。')
            is_notice = False
            
        ### Post processing (Remove old CSV files on docker)
        azure_blob_file_downloader.delete_old_local_file()
        elapsed_time = timeformat(time.time() - start)
        logger.info(f'Process all finished. Total time:{elapsed_time}')
        # Success Message
        if is_notice:
            message_list.append(f' - MKW食のトレンドPack 処理が完了しました。 処理時間:{elapsed_time}')
            notice_client.create_success_notice('  \n'.join(message_list))
    except Exception as ex:
        logger.error(f'Exception Occured:{ex}')
        # Error Occured Message
        notice_client.create_fail_notice(str(ex))

def timeformat(time):
    # minutes
    minutes = format(int(time / 60), '02d')
    seconds = format(int(time % 60), '02d')
    return f'{minutes}分{seconds}秒'

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
