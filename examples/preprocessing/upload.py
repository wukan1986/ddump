# pip install webdavclient3
from webdav3.client import Client


def main():
    """
    数据生成完成后，通过WebDAV上传到NAS
    """
    options = {
        'webdav_hostname': "http://192.168.31.11:5005",
        'webdav_login': "login",
        'webdav_password': "password"
    }
    client = Client(options)
    client.upload_sync(remote_path="public/data/data5.parquet", local_path=r"F:\preprocessing\data5.parquet")

    print("done")


if __name__ == '__main__':
    main()
