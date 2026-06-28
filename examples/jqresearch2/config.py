import pathlib
from http.cookies import SimpleCookie
from pprint import pprint

from jupyter_kernel_client import KernelClient

SERVER_URL = "https://www.joinquant.com/user/12345678901"

COOKIE = 'user-12345678901=2|1:0|10:1782612273|16:user-12345678901|48:NzA0ODFkNzItYWJkYi00Njg4LThkMDQtN2Y0M2NmODY0NGYw|52b0c3efe3112d34e67eb2728a1fd231aceb117a575d90177672553f67a847b6; uid=wKiXm2jC68o2sUDuZ0rwAg==; getStrategy=1; _xsrf=2|a0420526|3ad949c771bac9911b1012232f89763b|1782461011; token=2824ee78d21e17124b752edb6905c908b3892e78; PHPSESSID=io3t5iqtd22g6lbgfb47m6bb94'

HEADERS = {'Cookie': COOKIE, 'X-XSRFToken': SimpleCookie(COOKIE)['_xsrf'].value}

DATA_ROOT = pathlib.Path(r'D:\data\jqresearch')
DATA_ROOT_AKSHARE = pathlib.Path(r'D:\data\akshare')

# 未指定kernel_id时，获取!pwd是/，导致导入工作目录下库失败
with KernelClient(server_url=SERVER_URL, token=None, headers=HEADERS) as kernel:
    pprint(kernel.list_kernels())
    KERNEL_ID = kernel.list_kernels()[0]['id']
