import multiprocessing as mp
from threading import Thread
from tqdm import tqdm
from functools import partial


from .managers import ThreadManager, ProcessManager


__all__ = [
    'create_pool',
    'pool_map',
    'bmap',
]


def create_pool(
    n_procs,
    n_pre_threads,
    n_intra_threads,
    n_post_threads,
    preload,
    pre_fnc,
    intra_fnc,
    post_fnc,
    pre_kwargs,
    intra_kwargs,
    post_kwargs,
):
    procs = list()
    try:
        q_to = mp.Queue()
        q_from = mp.Queue()

        for _ in range(n_procs):
            procs.append(mp.Process(
                target=ProcessManager,
                kwargs={
                    'q_in': q_to,
                    'q_out': q_from,
                    'n_pre_threads': n_pre_threads,
                    'n_intra_threads': n_intra_threads,
                    'n_post_threads': n_post_threads,
                    'preload': preload,
                    'pre_fnc': pre_fnc,
                    'intra_fnc': intra_fnc,
                    'post_fnc': post_fnc,
                    'pre_kwargs': pre_kwargs,
                    'intra_kwargs': intra_kwargs,
                    'post_kwargs': post_kwargs,
                },
            ))
            procs[-1].start()
    except Exception as e:
        print(e)
        for proc in procs:
            proc.join()
    return q_to, q_from, procs


def pool_map(
    data,
    preload,
    q_to,
    q_from,
    procs,
    return_result,
    report_tqdm=True,
):
    load = 0
    finished = 0
    todo = len(data)
    if report_tqdm:
        pbar = tqdm(total=len(data), smoothing=0.1)
    if return_result:
        result = list()
    try:
        while finished < todo:
            if len(data) > 0 and load < preload:
                q_to.put(data.pop())
                load += 1
            if not q_from.empty():
                res = q_from.get()
                load -= 1
                finished += 1
                if return_result:
                    result.append(res)
                if report_tqdm:
                    pbar.update()
        for _ in range(len(procs)):
            q_to.put('end')
    except KeyboardInterrupt:
        for _ in range(len(procs)):
            q_to.put('kill')
    except Exception as e:
        print(e)
    finally:
        for proc in procs:
            proc.join()
        if report_tqdm:
            pbar.close()
    if return_result:
        return result


def bmap(
    n_procs,
    data,
    pre_threads=1,
    intra_threads=1,
    post_threads=1,
    preload=10,
    pre_fnc=None,
    intra_fnc=None,
    post_fnc=None,
    pre_kwargs=None,
    intra_kwargs=None,
    post_kwargs=None,
    return_result=False,
    report_tqdm=True,
):
    q_to, q_from, procs = create_pool(
        n_procs=n_procs,
        n_pre_threads=pre_threads,
        n_intra_threads=intra_threads,
        n_post_threads=post_threads,
        preload=preload,
        pre_fnc=pre_fnc,
        intra_fnc=intra_fnc,
        post_fnc=post_fnc,
        pre_kwargs=pre_kwargs,
        intra_kwargs=intra_kwargs,
        post_kwargs=post_kwargs,
    )
    result = pool_map(
        data=data,
        preload=preload,
        q_to=q_to,
        q_from=q_from,
        procs=procs,
        return_result=return_result,
        report_tqdm=report_tqdm,
    )
    return result

