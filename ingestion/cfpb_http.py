"""HTTP-запрос к API CFPB через системный curl.

Зачем: сайт CFPB за защитой от ботов. На стенде curl получает 200, а python-requests 403
(даже с одинаковым User-Agent). Поэтому запрос делает curl, а Python разбирает ответ.
Ходим последовательно и редко: по одному запросу за раз.
"""
import json
import subprocess
import time


class Resp:
    def __init__(self, status, body, ctype):
        self.status_code, self.text, self.headers = status, body, {"content-type": ctype}
        self.content = body.encode()

    def json(self):
        return json.loads(self.text)


def get(url, params=None, timeout=30, retries=3):
    """GET через curl. Повторы на сетевых ошибках, 429 и 5xx; остальные 4xx возвращаются сразу."""
    cmd = ["curl", "-sS", "-G", "-m", str(timeout), url,
           "-w", "\n__STATUS__%{http_code}\n__CTYPE__%{content_type}"]
    for k, v in (params or {}).items():
        for item in (v if isinstance(v, (list, tuple)) else [v]):   # список = повторяющийся параметр (product=A&product=B)
            cmd += ["--data-urlencode", f"{k}={item}"]
    last = None
    for attempt in range(1, retries + 1):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode == 0 and "__STATUS__" in p.stdout:
            body, _, tail = p.stdout.rpartition("\n__STATUS__")
            status, _, ctype = tail.partition("\n__CTYPE__")
            r = Resp(int(status), body, ctype.strip())
            if r.status_code == 200 or (r.status_code != 429 and r.status_code < 500):
                return r
            last = f"HTTP {r.status_code}"
        else:
            last = f"curl код {p.returncode}: {p.stderr.strip()[:120]}"
        if attempt < retries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"запрос не удался: {last}")
