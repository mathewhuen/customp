import multiprocessing as mp
from threading import Thread
from functools import partial


from customp import get, nmax, signal_override
from .utils import PreIntraPost


__all__ = ['ThreadManager', 'ProcessManager']


class ThreadManager(Thread):
    """Custom thread with continuous run method"""
    def __init__(
        self,
        in_q,
        out_q,
        fnc,
        fnc_kwargs=None,
        *args,
        **kwargs,
    ):
        super(ThreadManager, self).__init__(*args, **kwargs)
        self.in_q = in_q
        self.out_q = out_q
        self.fnc = fnc
        self.fnc_kwargs = get(fnc_kwargs, dict())

    def run(self):
        while True:
            msg = self.in_q.get()
            if msg == 'end':
                break
            res = self.fnc(msg, *self.fnc_kwargs)
            self.out_q.put(res)


class ProcessManager:
    """Manager for process"""
    def __init__(
        self,
        q_in,
        q_out,
        n_pre_threads=None,
        n_intra_threads=None,
        n_post_threads=None,
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
        pre_to = intra_to = post_to = None
        pre_from = intra_from = post_from = None
        pre_threads = intra_threads = post_threads = None
        _data_start = _data_end = None

        if pre_fnc is not None:
            n_pre_threads = get(n_pre_threads, 1)
            pre_to, pre_from, pre_threads = self.create_threads(
                to_threads=None,
                n_threads=n_pre_threads,
                fnc=pre_fnc,
                fnc_kwargs=pre_kwargs,
            )
            _data_start = pre_to
            _data_end = pre_from
        if intra_fnc is not None:
            n_intra_threads = get(n_intra_threads, 1)
            intra_to, intra_from, intra_threads = self.create_threads(
                to_threads=_data_end,
                n_threads=n_intra_threads,
                fnc=intra_fnc,
                fnc_kwargs=intra_kwargs,
            )
            if _data_start is None:
                _data_start = intra_to
            _data_end = intra_from
        if post_fnc is not None:
            n_post_threads = get(n_post_threads, 1)
            post_to, post_from, post_threads = self.create_threads(
                to_threads=_data_end,
                n_threads=n_post_threads,
                fnc=post_fnc,
                fnc_kwargs=post_kwargs,
            )
            if _data_start is None:
                _data_start = post_to
            _data_end = post_from

        self.max_threads = nmax(n_pre_threads, n_intra_threads, n_post_threads)
        assert self.max_threads > 0
        if _data_start is None:
            # no processing: return results as they come
            self.data_start = self.data_end = mp.Queue
        else:
            self.data_start = _data_start
            self.data_end = _data_end
        self._to = PreIntraPost(pre_to, intra_to, post_to)
        self._from = PreIntraPost(pre_from, intra_from, post_from)
        self.threads = PreIntraPost(pre_threads, intra_threads, post_threads)
        self.preload = preload
        self.cnt = 0  # PreIntraPost(0, 0, 0)
        self.run()

    def create_threads(self, to_threads, n_threads, fnc, fnc_kwargs):
        _to_threads = get(to_threads, mp.Queue())
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
        """
        while True:
            # get data, parse for end, add to queue
            if self.cnt < self.preload and not self.q_in.empty():
                try:
                    data = self.q_in.get(timeout=0.1)
                except:
                    continue
                if isinstance(data, str) and data in ['end', 'kill']:
                    self.end_threads()
                    if data == 'kill':
                        print('\nProcess Killed')
                    break
                self.data_start.put(data)
                self.cnt += 1

            # check for finished data
            if not self.data_end.empty():
                res = self.data_end.get()
                self.cnt -= 1
                self.q_out.put(res)

