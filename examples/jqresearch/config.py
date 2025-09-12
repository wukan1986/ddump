import pathlib

# 使用ksrpc代替原生接口下载数据
from ksrpc.connections.websocket import WebSocketConnection  # noqa
from ksrpc.connections.http import HttpxConnection  # noqa
from ksrpc.rpc_client import RpcClient

TOKEN = 'secret-token-2'
URL = 'ws://127.0.0.1:8000/ws/admin?room=HA9527'
conn = WebSocketConnection(URL, token=TOKEN)

URL = 'http://127.0.0.1:7001/api/file'
conn = HttpxConnection(URL, token=TOKEN)

jq = RpcClient('jqresearch.api', conn, async_local=False, async_remote=False)
jq.cache_get = False
jq.cache_expire = 5
jq.recv_timeout = 30

# StateException('Interruptingcow can only be used from the MainThread.',)
# 由于query相关操作无法异步，所以只能指定async_remote=False
jqr = RpcClient('jqresearch_query', conn, async_local=False, async_remote=False)
jqr.cache_get = False
jqr.cache_expire = 5
jqr.recv_timeout = 30

DATA_ROOT = pathlib.Path(r'D:\data\jqresearch')
DATA_ROOT_AKSHARE = pathlib.Path(r'D:\data\akshare')
