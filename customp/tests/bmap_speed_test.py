from customp import bmap


from time import time as now, sleep
from functools import partial
import multiprocessing as mp
from tqdm import tqdm
from copy import deepcopy as dc


def io_fnc(nm):
    n, m = nm
    sleep(n)
    return m


def cpu_fnc(n):
    out = 1
    for _ in range(n):
        out = out * 3.1415 % n
    return out


def combined_fnc(nm):
    n = io_fnc(nm)
    out = cpu_fnc(n)
    return out


def mp_map(n_procs, fnc, data, use_tqdm):
    with mp.Pool(n_procs) as p:
        if use_tqdm:
            pbar = tqdm(total=len(data))
        for _ in p.imap(fnc, data):
            if use_tqdm:
                pbar.update()
    return None


def stest_(
    reps,
    data,
    n_procs=3,
    bmap_pre_threads=10,
    bmap_intra_threads=2,
    bmap_preload=40,
    use_tqdm=False,
):
    bmap_times = list()
    for _ in range(reps):
        time0= now()
        try:
            bmap(
                n_procs=n_procs,
                data=dc(data),
                pre_threads=bmap_pre_threads,
                pre_fnc=io_fnc,
                intra_threads=bmap_intra_threads,
                intra_fnc=cpu_fnc,
                preload=bmap_preload,
                report_tqdm=use_tqdm,
            )
        except Exception as e:
            print(e)
            breakpoint()
        bmap_times.append((time0, now()))
    bmap_times = [end - start for start, end in bmap_times]
    bmap_mean = sum(bmap_times) / len(bmap_times)

    mpmap_times = list()
    for _ in range(reps):
        time0 = now()
        try:
            mp_map(n_procs=n_procs, fnc=combined_fnc, data=dc(data), use_tqdm=use_tqdm)
        except Exception as e:
            print(e)
            breakpoint()
        mpmap_times.append((time0, now()))
    mpmap_times = [end - start for start, end in mpmap_times]
    mpmap_mean = sum(mpmap_times) / len(mpmap_times)

    return bmap_mean, mpmap_mean


def stest(reps, n_procs):
    print('\nProcessing Case 1: long load, short processing')
    data = [(1, 3)] * 200
    bmap_mean, mpmap_mean = stest_(reps, data, n_procs)
    print('bmap mean time:\n{}\nmp/imap mean time:\n{}'.format(bmap_mean, mpmap_mean))
    
    print('\nProcessing Case 2: short load, short processing')
    data = [(0.05, 3)] * 200
    bmap_mean, mpmap_mean = stest_(reps, data, n_procs)
    print('bmap mean time:\n{}\nmp/imap mean time:\n{}'.format(bmap_mean, mpmap_mean))

    print('\nProcessing Case 3: short load, long processing')
    data = [(0.05, 10_000)] * 200
    bmap_mean, mpmap_mean = stest_(reps, data, n_procs)
    print('bmap mean time:\n{}\nmp/imap mean time:\n{}'.format(bmap_mean, mpmap_mean))

    print('\nProcessing Case 4: long load, long processing')
    data = [(1, 10_000)] * 200
    bmap_mean, mpmap_mean = stest_(reps, data, n_procs)
    print('bmap mean time:\n{}\nmp/imap mean time:\n{}'.format(bmap_mean, mpmap_mean))

if __name__ == '__main__':
    reps = 3
    n_procs = mp.cpu_count()
    if n_procs > 4:
        n_procs -= 1
    stest(reps, n_procs)

