from dataclasses import dataclass
from typing import Optional
from collections import deque

def suffix_str(s, n):
    if n <= 0: return ""
    return s[-n:] if n <= len(s) else s

def prefix_str(s, n):
    if n <= 0: return ""
    return s[:n] if n <= len(s) else s

def reverse_str(s): return s[::-1]

@dataclass
class State:
    L: str
    H: str
    R: str
    display: str = ""
    bunsetsu_count: int = 0

    def is_palindrome_state(self): return self.L == "" and self.R == ""
    def is_valid_palindrome(self): return self.H == reverse_str(self.H)
    def verify_state(self):
        c = self.L + self.H + self.R
        return c == reverse_str(c)

def generate_initial_states(seed_kana, seed_display):
    states, seen = [], set()
    n = len(seed_kana)

    def make_state(left_part, right_part):
        need_L = reverse_str(right_part)
        ll, nl = len(left_part), len(need_L)
        if ll == 0 and nl == 0: L, R = "", ""
        elif ll == 0: L, R = need_L, ""
        elif nl == 0: L, R = "", reverse_str(left_part)
        else:
            common = min(ll, nl)
            if left_part[:common] != need_L[:common]: return None
            if ll <= nl: L, R = need_L[ll:], ""
            else: L, R = "", reverse_str(left_part[nl:])
        s = State(L=L, H=seed_kana, R=R, display=seed_display, bunsetsu_count=1)
        return s if s.verify_state() else None

    def add(s):
        if not s: return
        key = (s.L, s.H, s.R)
        if key not in seen:
            seen.add(key); states.append(s)

    for i in range(n): add(make_state(seed_kana[:i], seed_kana[i+1:]))
    for i in range(n+1): add(make_state(seed_kana[:i], seed_kana[i:]))
    return states

def extend_left(state, w_kana, w_display):
    L = state.L
    if not L: return None
    wl, ll = len(w_kana), len(L)
    if wl <= ll and suffix_str(L, wl) == w_kana:
        ns = State(L=prefix_str(L, ll-wl), H=w_kana+state.H, R="",
                   display=w_display+state.display, bunsetsu_count=state.bunsetsu_count+1)
        if ns.verify_state(): return ns
    if ll <= wl and suffix_str(w_kana, ll) == L:
        ns = State(L="", H=w_kana+state.H, R=reverse_str(prefix_str(w_kana, wl-ll)),
                   display=w_display+state.display, bunsetsu_count=state.bunsetsu_count+1)
        if ns.verify_state(): return ns
    return None

def extend_right(state, w_kana, w_display):
    R = state.R
    if not R: return None
    wl, rl = len(w_kana), len(R)
    if wl <= rl and prefix_str(R, wl) == w_kana:
        ns = State(L="", H=state.H+w_kana, R=suffix_str(R, rl-wl),
                   display=state.display+w_display, bunsetsu_count=state.bunsetsu_count+1)
        if ns.verify_state(): return ns
    if rl <= wl and prefix_str(w_kana, rl) == R:
        ns = State(L=reverse_str(suffix_str(w_kana, wl-rl)), H=state.H+w_kana, R="",
                   display=state.display+w_display, bunsetsu_count=state.bunsetsu_count+1)
        if ns.verify_state(): return ns
    return None

def search_palindromes(seed_kana, seed_display, bunsetsu_db, max_bunsetsu=5, max_results=20):
    results = []
    queue = deque(generate_initial_states(seed_kana, seed_display))
    visited = set()
    while queue and len(results) < max_results:
        state = queue.popleft()
        key = (state.L, state.H, state.R)
        if key in visited: continue
        visited.add(key)
        if state.is_palindrome_state():
            if state.bunsetsu_count >= 2 and state.is_valid_palindrome():
                results.append(state)
            continue
        if state.bunsetsu_count >= max_bunsetsu: continue
        for w_kana, w_display in bunsetsu_db:
            if state.L:
                ns = extend_left(state, w_kana, w_display)
                if ns and (ns.L, ns.H, ns.R) not in visited: queue.append(ns)
            if state.R:
                ns = extend_right(state, w_kana, w_display)
                if ns and (ns.L, ns.H, ns.R) not in visited: queue.append(ns)
    return results
