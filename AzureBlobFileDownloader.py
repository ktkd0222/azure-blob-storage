elf):
    # Logger
    self._logger = getLogger(__name__)
    self._logger.addHandler(NullHandler())
    self._logger.setLevel(DEBUG)
    self._logger.propagate = True
    # Azure Blob / Urllib3のログが大量に出力されるのでWARNING以上に設定
    getLogger("azure").setLevel(WARNING)
    getLogger("urllib3").setLevel(WARNING)

    # Initialize the connection to Azure storage account
    self.blob_service_client =  BlobServiceClient.from_connection_string(cst.const.BLOB_CONNECTION_STRING)
    self.my_container = self.blob_service_client.get_container_client(cst.const.CONTAINER_NAME)
    self.target_folder = '{0:%Y%m}'.format(datetime.today())
    self.my_hash = HashCalculate()
    self.file_check = FileChecker()
    self.old_folder = '{0:%Y%m%d%H%M%S}'.format(datetime.today())
    self.delete_target_folder = '{0:%Y%m}'.format(datetime.today() - relativedelta(months=6))

    self._logger.info(f'Initialize AzureBlobFileDownloader. Container Name:{cst.const.CONTAINER_NAME}')

  # save_blob
  # ダウンロードしたBlobファイルをローカルに保存します（既にファイルが存在した場合は上書処理）
  # ローカル保存場所：cst.const.OUTPUT_FILE_PATH
  # ※今回は未使用
  # [引数]
  # - file_name : CSVファイル名称
  # - file_content : ファイル内容（byte[]）
  def save_blob(self,file_name,file_content):
    # Get full path to the file
    download_file_path = os.path.join(cst.const.OUTPUT_FILE_PATH, file_name)
 
    # for nested blobs, create local path as well!
    os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
 
    with open(download_file_path, "wb") as file:
      file.write(file_content)
  
  # save_target_month_blob
  # ダウンロードしたBlobファイルをローカルに保存します
  # ローカル保存場所：cst.const.OUTPUT_FILE_PATH
  # [引数]
  # - file_name : CSVファイル名称
  # - file_content : ファイル内容（byte[]）
  def save_target_month_blob(self,file_name,file_content):
    # Get full path to the file
    download_file_path = os.path.join(cst.const.OUTPUT_FILE_PATH, file_name)
 
    # for nested blobs, create local path as well!
    os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

    # ファイル作成
    with open(download_file_path, "wb") as file:
        file.write(file_content)

  # download_all_blobs_in_container
  # コンテナ内のCSVファイルを全てダウンロードします
  # ※今回は未使用
  def download_all_blobs_in_container(self):
    my_blobs = self.my_container.list_blobs()
    for blob in my_blobs:
        self._logger.info(f'Downloading Blob : {blob.name}')
        bytes = self.my_container.get_blob_client(blob).download_blob().readall()
        self.save_blob(blob.name, bytes)
 
  # donwload_target_month_blobs_in_container
  # 対象月のフォルダのみのCSVファイルをダウンロードします
  # 対象月は実行時の日時から自動判定
  # 保存処理は別メソッドを呼び出ししています
  def download_target_month_blobs_in_container(self):
    file_number = 0
    file_names  = []

    # Download From 
    my_blobs = self.my_container.list_blobs()
    self._logger.info(f'Download target blob -----> {os.path.join(self.target_folder, "*.csv")}')
    for blob in my_blobs:
        if blob.name.startswith(self.target_folder) :
            dirname, filename = os.path.split(blob.name)
            if filename in cst.const.FILE_NAME_LIST:
                if self.file_check.is_file_download(blob.name, blob.last_modified):
                    start = time.time()
                    self._logger.info(f'Download blob : {blob.name}')
                    bytes = self.my_container.get_blob_client(blob).download_blob().readall()
                    elapsed_time = time.time() - start
                    self._logger.info(f'Download finished. Download Time:{elapsed_time}[sec]')
                    self.save_target_month_blob(blob.name, bytes)
                    file_number += 1
                    file_names.append(blob.name)
                else:
                    self._logger.info(f'Download and local file is not change. Ignore download blob : {blob.name}')
            else:
                self._logger.debug(f'Ignore blob : {blob.name}')
        else:
            self._logger.debug(f'Ignore blob : {blob.name}')
    return (file_number, file_names)

  # file_check_target_month_blobs_in_container
  # 対象月のフォルダを検索し、対象ファイルが全て存在していることを確認します
  # 対象月は実行時の日時から自動判定
  def file_check_target_month_blobs_in_container(self):
    file_number = 0
    file_names  = []

    # Download From 
    my_blobs = self.my_container.list_blobs()
    self._logger.info(f'Check target blob -----> {os.path.join(self.target_folder, "*.csv")}')
    for blob in my_blobs:
        if blob.name.startswith(self.target_folder) :
            dirname, filename = os.path.split(blob.name)
            if filename in cst.const.FILE_NAME_LIST:
                file_number += 1
                
    return True if file_number == 4 else False

  # delete_local_file
  # ファイルが中途半端にダウンロードされた場合のローカルファイル削除処理
  def delete_local_file(self):
    self._logger.info(f'Local file delete...')
    # Get local csv file path
    local_file_path = os.path.join(cst.const.OUTPUT_FILE_PATH, self.target_folder, '*.csv')
    files = glob.glob(local_file_path)

    for file in files:
        if os.path.basename(file) in cst.const.FILE_NAME_LIST:
            os.remove(file)
            self._logger.info(f'Delete partial csv files. file:{file}')

  # delete_old_local_file
  # 後処理用に過去ローカルファイル削除処理
  def delete_old_local_file(self):
    self._logger.info(f'Local old file delete...')
    delete_folder = os.path.join(cst.const.OUTPUT_FILE_PATH, self.delete_target_folder)
    if os.path.isdir(delete_folder) :
        shutil.rmtree(delete_folder)
        self._logger.info(f'Delete old files. folder:{delete_folder}')
    else:
        self._logger.info(f'Delete target folder NOT exists. folder:{delete_folder}')
