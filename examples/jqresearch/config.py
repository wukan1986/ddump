import pathlib

# 使用ksrpc代替原生接口下载数据
from ksrpc.connections.websocket import WebSocketConnection
from ksrpc.rpc_client import RpcClient

TOKEN = 'secret-token-2'
URL = 'ws://127.0.0.1:8000/ws/admin?room=9527'
URL = 'ws://1.117.86.56:8000/ws/admin?room=9527'

conn = WebSocketConnection(URL, token=TOKEN)
jq = RpcClient('jqresearch.api', conn, async_local=False)
jq.cache_get = False
jq.cache_expire = 60

# StateException('Interruptingcow can only be used from the MainThread.',)
# 由于query相关操作无法异步，所以只能指定async_remote=False
jqr = RpcClient('jqresearch_query', conn, async_local=False, async_remote=False)
jqr.cache_get = False
jqr.cache_expire = 60

DATA_ROOT = pathlib.Path('../..').resolve() / 'data/jqresearch'
DATA_ROOT = pathlib.Path(r'D:\data\jqresearch')
DATA_ROOT_AKSHARE = pathlib.Path(r'D:\data\akshare')
