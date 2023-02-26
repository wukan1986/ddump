import shutil

import pandas as pd
from loguru import logger


def merge_files_to_file(path, files,
                        ignore_index=True,
                        delete_src=False,
                        single_overwrite=True):
    """合并件列表到文件

    Parameters
    ----------
    path: pathlib.Path
        目标路径
    files: list of Path
        源路径列表
    ignore_index: bool
        合并时是否忽略索引。索引没有意义时忽略能加速
    delete_src: bool
        是否删除源文件
    single_overwrite: bool
        单文件是否进行覆盖

    """
    if len(files) == 0:
        return

    if len(files) == 1:
        if path == files[0]:
            # 同一文件，没有必要合并
            return
        else:
            if path.exists() and not single_overwrite:
                logger.info('单路径，已存在，跳过 {}', path)
                return
            else:
                logger.info('单路径，直接覆盖 {}', path)
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(files[0], path)
                return

    if path.exists():
        logger.info('合并目标，已存在，跳过 {}', path)
        return

    # 加载
    dfs = []
    for f in files:
        dfs.append(pd.read_parquet(f))

    if len(dfs) == 0:
        return

    h, t = files[0], files[-1]
    if path.parent == t.parent:
        # 目录相同，必须删，否则混一起没法看
        delete_src = True

    # 合并,希望内存够
    logger.info('合并 {} 至 {} 等 {}个文件。是否删除?{}', h.name, t.name, len(files), delete_src)
    dfs = pd.concat(dfs, ignore_index=ignore_index)

    file_temp = path.with_suffix('.tmp')
    logger.info('写入文件：{}', file_temp)

    # 写入临时文件
    path.parent.mkdir(parents=True, exist_ok=True)
    dfs.to_parquet(file_temp, compression='zstd')

    # 全删
    if delete_src:
        for f in files:
            f.unlink(missing_ok=True)

    # 改名
    file_temp.rename(path)


def merge_files_dict(files_dict,
                     ignore_index=False, delete_src=False):
    """合并特殊字典。

    key为路径
    value为列表
    """
    for i, (k, v) in enumerate(files_dict.items()):
        # 最后5个单文件总是试着覆盖
        single_overwrite = i >= len(files_dict) - 5
        merge_files_to_file(k, v, ignore_index, delete_src, single_overwrite)
