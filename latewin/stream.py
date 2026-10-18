"""Streaming processor: watermark, allowed lateness, retractions, side output.

Messages are tuples (kind, window_id, version, value) with kind in
{"emit", "retract"}. The Sink makes delivery idempotent and order-tolerant.
"""
import heapq

from .windows import assign

INF = float("inf")


class Sink:
    """Materialised view. Duplicate (kind, window, version) messages are ignored;
    the highest version per window wins regardless of delivery order, and a
    retract of version v dominates an emit of the same version."""

    def __init__(self):
        self.view, self.ver, self.seen = {}, {}, set()
        self.applied = self.dupes = 0

    def apply(self, msg):
        kind, wid, ver, val = msg
        if (kind, wid, ver) in self.seen:
            self.dupes += 1
            return False
        self.seen.add((kind, wid, ver))
        cur = self.ver.get(wid, 0)
        if kind == "emit":
            if ver <= cur:
                return False
            self.ver[wid], self.view[wid] = ver, val
        else:
            if ver < cur:
                return False
            self.ver[wid] = ver
            self.view.pop(wid, None)
        self.applied += 1
        return True


class Processor:
    """Feed events in arrival order with process(); call flush() at end of stream."""

    def __init__(self, spec, delay, lateness=0):
        self.spec, self.delay, self.lateness = spec, delay, lateness
        self.wm, self.clock = -INF, 0
        self.win, self.sess, self.heap = {}, {}, []  # wid -> [count, sum, fired]
        self.ver, self.live, self.first = {}, set(), {}  # first: wid -> (value, latency)
        self.out, self.side, self.updates = [], [], 0

    def process(self, e):
        self.clock = e.arrival_time
        touched = self._session(e) if self.spec.kind == "session" else self._fixed(e)
        self.wm = max(self.wm, e.event_time - self.delay)
        for wid in touched:
            if self.win[wid][2]:
                self._emit(wid)  # window already fired: publish a new version
        while self.heap and self.heap[0][0] <= self.wm:
            _, wid = heapq.heappop(self.heap)
            if wid in self.win and not self.win[wid][2]:
                self._emit(wid)

    def flush(self):
        for wid in sorted(self.win):
            if not self.win[wid][2]:
                self._emit(wid)

    def _fixed(self, e):
        ws = [(e.key, s, en) for s, en in assign(self.spec, e.event_time)]
        ok = [w for w in ws if w[2] + self.lateness > self.wm]
        if len(ok) < len(ws):
            self.side.append(e)
        for w in ok:
            if w not in self.win:
                self.win[w] = [0, 0, False]
                heapq.heappush(self.heap, (w[2], w))
            self.win[w][0] += 1
            self.win[w][1] += e.value
        return ok

    def _session(self, e):
        t, g = e.event_time, self.spec.gap
        lst = self.sess.setdefault(e.key, [])
        over = [w for w in lst if w[1] < t + g and t < w[2]]
        s, en = min([t] + [w[1] for w in over]), max([t + g] + [w[2] for w in over])
        if en + self.lateness <= self.wm:
            self.side.append(e)
            return []
        new, c, tot, fired = (e.key, s, en), 1, e.value, False
        for w in over:
            st = self.win.pop(w)
            lst.remove(w)
            c, tot, fired = c + st[0], tot + st[1], fired or st[2]
            if w != new and w in self.live:  # old session id disappears: retract it
                self.out.append(("retract", w, self.ver[w], None))
                self.live.discard(w)
        self.win[new] = [c, tot, fired]
        lst.append(new)
        heapq.heappush(self.heap, (en, new))
        return [new]

    def _emit(self, wid):
        st, v = self.win[wid], self.ver.get(wid, 0)
        if wid in self.live:
            self.out.append(("retract", wid, v, None))
            self.updates += 1
        self.ver[wid] = v + 1
        self.live.add(wid)
        st[2] = True
        val = (st[0], st[1])
        self.out.append(("emit", wid, v + 1, val))
        self.first.setdefault(wid, (val, max(0, self.clock - wid[2])))


def run_stream(events, spec, delay, lateness=0):
    p = Processor(spec, delay, lateness)
    for e in events:
        p.process(e)
    p.flush()
    return p
