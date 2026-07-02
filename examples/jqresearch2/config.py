import pathlib
import re
from http.cookies import SimpleCookie
from pprint import pprint

from jupyter_kernel_client import KernelClient

COOKIE = 'user-12345678901=2|1:0|10:1782953117|16:user-12345678901|48:NDM3YzcyMjgtMGI3Zi00N2JjLTg3N2ItZWNkMGQyN2I2NThi|b42efa665f4afabde62b13ed06e634c4d4460c125737886dc5cf2d198eee56ae; uid=wKgyrWpFsz1+TAXEuiVIAg==; token=406d04b88057c53da03f2ee40d9acdc96ce6df6e; PHPSESSID=ec5aet2nia2s0ect53a1kucma5; _xsrf=2|596e71d1|ddc54c7ec6cee0b5a79f19e0191a98a3|1782953117'
HEADERS = {'Cookie': COOKIE, 'X-XSRFToken': SimpleCookie(COOKIE)['_xsrf'].value}
UID = re.search(r'user-(\d+)=', COOKIE).group(1)
SERVER_URL = f"https://www.joinquant.com/user/{UID}"

DATA_ROOT = pathlib.Path(r'D:\data\jqresearch')
DATA_ROOT_AKSHARE = pathlib.Path(r'D:\data\akshare')

# 未指定kernel_id时，获取!pwd是/，导致导入工作目录下库失败
with KernelClient(server_url=SERVER_URL, token=None, headers=HEADERS) as kernel:
    pprint(kernel.list_kernels())
    KERNEL_ID = kernel.list_kernels()[0]['id']
