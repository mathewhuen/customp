# TODO
#   - fix None vs 1 for n_threads for pre, intra, post


import multiprocessing as mp
from threading import Thread
from collections import deque
from tqdm import tqdm
import signal
from functools import partial


def ident(val, default=None):
    """Return the given value, replace with default if None"""
    return val if val is not None else default


class ThreadManager(Thread):
    """Custom thread with continuous run method"""
    def __init__(
        self,
        from_parent,
        to_parent,
        fnc,
        fnc_kwargs=None,
        *args,
        **kwargs,
    ):
        super(ThreadManager, self).__init__(*args, **kwargs)
        self.from_parent = from_parent
        self.to_parent = to_parent
        self.fnc = fnc
        self.fnc_kwargs = ident(fnc_kwargs, dict())

    def run(self):
        while True:
            msg = self.from_parent.get()
            if msg == 'end':
                break
            res = self.fnc(msg, *self.fnc_kwargs)
            self.to_parent.put(res)


def signal_override():
    """Force the current process to ignore ctrl-c"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class PreIntraPost:
    """Very simple wrapper for a triple of objects"""
    def __init__(self, pre, intra, post):
        self.pre = pre
        self.intra = intra
        self.post = post

    def apply(self, fnc, pre=True, intra=True, post=True):
        """
        Apply fnc to each self.pre, self.intra, self.post if pre, intra,
        post is True, respectively.
        """
        if pre and self.pre is not None:
            fnc(self.pre)
        if intra and self.intra is not None:
            fnc(self.intra)
        if post and self.post is not None:
            fnc(self.post)

    def __getitem__(self, k):
        if k == 'pre':
            return self.pre
        if k == 'intra':
            return self.intra
        if k == 'post':
            return self.post


def nmax(*xs):
    "Ignore None and calc max. If all None, return -inf"
    m = -float('inf')
    for x in xs:
        if x is None:
            continue
        m = max(m, x)
    return m


class ProcessManager:
    """Manager for process"""
    def __init__(
        self,
        q_in,
        q_out,
        pre_threads=None,
        intra_threads=1,
        post_threads=None,
        preload=10,
        pre_fnc=None,
        intra_fnc=None,
        post_fnc=None,
        pre_kwargs=None,
        intra_kwargs=None,
        post_kwargs=None,
    ):
        signal_override()
        self.q_in = q_in
        self.q_out = q_out
        self.n_threads = PreIntraPost(pre_threads, intra_threads, post_threads)
        self.max_threads = nmax(pre_threads, intra_threads, post_threads)
        assert self.max_threads > 0
        pre_to = intra_to = post_to = None
        pre_from = intra_from = post_from = None
        if pre_threads is not None:
            if pre_fnc is None:
                pre_fnc = lambda x: x
            pre_to, pre_from, pre_threads = self.create_threads(
                n_threads=pre_threads,
                fnc=pre_fnc,
                fnc_kwargs=pre_kwargs,
            )
        if intra_threads is not None:
            if intra_fnc is None:
                intra_fnc = lambda x: x
            intra_to, intra_from, intra_threads = self.create_threads(
                n_threads=intra_threads,
                fnc=intra_fnc,
                fnc_kwargs=intra_kwargs,
            )
        if post_threads is not None:
            if post_fnc is None:
                post_fnc = lambda x: x
            post_to, post_from, post_threads = self.create_threads(
                n_threads=post_threads,
                fnc=post_fnc,
                fnc_kwargs=post_kwargs,
            )
        self._to = PreIntraPost(pre_to, intra_to, post_to)
        self._from = PreIntraPost(pre_from, intra_from, post_from)
        self.threads = PreIntraPost(pre_threads, intra_threads, post_threads)
        self.preload = 10
        self.cnt = PreIntraPost(0, 0, 0)
        self.data = deque(list())
        self.run()

    def create_threads(self, n_threads, fnc, fnc_kwargs):
        _to_threads = mp.Queue()
        _from_threads = mp.Queue()
        _threads = list()
        for _ in range(n_threads):
            _threads.append(ThreadManager(_to_threads, _from_threads, fnc, fnc_kwargs))
            _threads[-1].start()
        return _to_threads, _from_threads, _threads

    def _send_end(self, to, n):
        for _ in range(n):
            to.put('end')

    def _join_threads(self, threads):
        for thread in threads:
            thread.join()

    def end_threads(self):
        self._to.apply(partial(self._send_end, n=self.max_threads))
        self.threads.apply(self._join_threads)

    def run(
        self,
    ):
        """
        Main processing loop for process.
        Each loop, check:
         - check for kill or end message
         - if data can be added to the pre queue
         - if there are pre results
         - if there are pre results that can be added to the intra queue
         - if there are intra results
         - if there are intra results that can be added to the post queue
         - if there are post results
         pre, intra, and post functions are
        """
        while True:
            # load data
            if not self.q_in.empty():

                # ! add preload/cnt check ! #

                data = self.q_in.get()
                if isinstance(data, str) and data in ['end', 'kill']:
                    self.end_threads()
                    if data == 'kill':
                        print('\nProcess Killed')
                    break
                self.data.append(data)

            # assign data to processing fncs
            res = None
            is_result = False
            res_post = None
            if self.n_threads.pre is not None:
                if self.cnt.pre <= self.preload and len(self.data) > 0:
                    self._to.pre.put(self.data.popleft())
                    self.cnt.pre += 1
                if not self._from.pre.empty():
                    res = self._from.pre.get()
                    self.cnt.pre -= 1
            if self.n_threads.intra is not None:  # ! assume >= 1 !#
                if self.cnt.intra <= self.preload and res is not None:  # ! just add directly ! #
                    self._to.intra.put(res)
                    self.cnt.intra += 1
                if not self._from.intra.empty():
                    res = self._from.intra.get()
                    self.cnt.intra -= 1
            if self.n_threads.post is not None:
                if self.cnt.post <= self.preload and res is not None:
                    self._to.post.put(res)
                    self.cnt.post += 1
                if not self._from.post.empty():
                    res_post = self._from.post.get()
                    self.cnt.post -= 1
                    is_result = True
            if is_result:
                self.q_out.put(res_post)


def create_pool(
    n_procs,
    pre_threads,
    intra_threads,
    post_threads,
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
                    'pre_threads': pre_threads,
                    'intra_threads': intra_threads,
                    'post_threads': post_threads,
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
):
    load = 0
    finished = 0
    todo = len(data)
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
):
    q_to, q_from, procs = create_pool(
        n_procs=n_procs,
        pre_threads=pre_threads,
        intra_threads=intra_threads,
        post_threads=post_threads,
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
    )
    if return_result:
        return result

