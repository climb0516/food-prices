#!/usr/bin/env python3
# Created by 青云 on 2026/10/21.
"""菜价物价 scraper — 新发地每日批发价、农业农村部全国批发价格指数与涨跌榜、国家统计局 CPI，输出 data.json。仅依赖 Python 标准库。"""
import html, json, re, time, urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

ROOT  = Path(__file__).parent
OUT   = ROOT / "data.json"
CST   = timezone(timedelta(hours=8))
NOW   = datetime.now(CST)
TODAY = NOW.date()
BACKFILL_DAYS = 90       # 新品种 / 首次运行回补多少天
KEEP_DAYS     = 400      # 单品价格保留多少天
CPI_MONTHS    = 24       # CPI 首次回补多少个月

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
      "Accept-Language": "zh-CN,zh;q=0.9"}

XFD  = "http://www.xinfadi.com.cn/getPriceData.html"
MOA  = "https://pfsc.agri.cn/price_portal"
STATS = "https://www.stats.gov.cn/sj/zxfb/"

# 新发地品种：(id, 显示名, 分组, 新发地品名, 规格 —— None 表示该品名所有「斤」报价取平均)
BASKET = [
    ("pork_lean", "猪肉（瘦）", "肉", "白条猪", "瘦"),
    ("pork_fat",  "猪肉（肥）", "肉", "白条猪", "肥"),
    ("beef",      "牛肉（后腿）", "肉", "牛后腿", None),
    ("mutton",    "羊肉片",     "肉", "羊肉片", None),
    ("chicken",   "鸡胸肉",     "肉", "鸡胸", None),
    ("egg",       "鸡蛋",       "蛋", "筐鸡蛋", None),
    ("egg_loose", "散鸡蛋",     "蛋", "散鸡蛋", None),
    ("cabbage",   "大白菜",     "菜", "大白菜", None),
    ("potato",    "土豆",       "菜", "土豆", ""),
    ("tomato",    "西红柿",     "菜", "番茄", None),
    ("cucumber",  "黄瓜",       "菜", "黄瓜", None),
    ("pepper",    "青椒",       "菜", "柿子椒", None),
    ("chili",     "尖椒",       "菜", "尖椒", None),
    ("eggplant",  "茄子",       "菜", "茄子", None),
    ("celery",    "芹菜",       "菜", "芹菜", None),
    ("spinach",   "菠菜",       "菜", "菠菜", None),
    ("rape",      "油菜",       "菜", "油菜", None),
    ("radish",    "白萝卜",     "菜", "白萝卜", None),
    ("carrot",    "胡萝卜",     "菜", "胡萝卜", None),
    ("onion",     "洋葱",       "菜", "葱头", "黄"),
    ("scallion",  "大葱",       "菜", "大葱", None),
    ("garlic",    "大蒜",       "菜", "大蒜", "干"),
    ("ginger",    "生姜",       "菜", "姜", "整姜"),
    ("leek",      "韭菜",       "菜", "韭菜", None),
    ("bean",      "豆角（豇豆）", "菜", "豇豆", None),
    ("apple",     "苹果（富士）", "果", "富士", None),
    ("banana",    "香蕉",       "果", "国产香蕉", None),
    ("pear",      "鸭梨",       "果", "鸭梨", None),
    ("orange",    "脐橙",       "果", "脐橙", None),
    ("grass_carp","草鱼",       "鱼虾", "草鱼", "750g-1000g"),
    ("carp",      "鲤鱼",       "鱼虾", "鲤鱼", "750g-1000g"),
    ("hairtail",  "带鱼（冻）",  "鱼虾", "带鱼", "300g-500g/冻"),
]

# CPI 公报表格里要的行（表格第一列的文字 → id）
CPI_ROWS = {
    "居民消费价格": "cpi", "其中：食品": "food", "食品": "food", "其中：不包括食品和能源": "core", "不包括食品和能源": "core",
    "粮食": "grain", "鲜菜": "veg", "其中：猪肉": "pork", "猪肉": "pork", "畜肉类": "meat", "牛肉": "beef", "羊肉": "mutton",
    "水产品": "aquatic", "蛋类": "egg", "鲜果": "fruit", "食用油": "oil",
}

# ── 工具 ──────────────────────────────────────────────────────────────────────

def fetch(url, data=None, headers=None, timeout=25, tries=3):
    """新发地偶尔返回 503（限流），失败后等几秒重试。"""
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers={**UA, **(headers or {})})
            return urllib.request.urlopen(req, timeout=timeout).read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            if i == tries - 1 or (isinstance(e, urllib.error.HTTPError) and e.code not in (429, 500, 502, 503, 504)):
                raise
            time.sleep(3 * (i + 1))

def clean(text):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text or ""))).strip()

def run(label, fn):
    try:
        res = fn()
        print(f"  ✓ {label:<12} {len(res):>4}")
        return res, ""
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:200]
        print(f"  ✗ {label:<12}    0 {err}")
        return None, err

def num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None

# ── 新发地 ────────────────────────────────────────────────────────────────────

def xfd_query(start, end, name="", limit=1000):
    rows, page = [], 1
    while True:
        body = urllib.parse.urlencode({"limit": limit, "current": page, "pubDateStartTime": start.strftime("%Y/%m/%d"),
                                       "pubDateEndTime": end.strftime("%Y/%m/%d"), "prodPcatid": "", "prodCatid": "",
                                       "prodName": name}).encode()
        d = json.loads(fetch(XFD, body, {"Content-Type": "application/x-www-form-urlencoded"}))
        rows += d.get("list") or []
        if len(rows) >= int(d.get("count") or 0) or not d.get("list") or page >= 10:
            return rows
        page += 1

def pick(rows, name, spec):
    """按品名 + 规格筛出「斤」报价，按日期取平均。"""
    by_day = {}
    for r in rows:
        if r.get("prodName") != name or r.get("unitInfo") != "斤":
            continue
        if spec is not None and (r.get("specInfo") or "") != spec:
            continue
        p = num(r.get("avgPrice"))
        if p is None or p <= 0:
            continue
        by_day.setdefault(r["pubDate"][:10], []).append(p)
    return {d: round(sum(v) / len(v), 2) for d, v in by_day.items()}

def pick_meta(rows, name, spec):
    """最近一个报价日的最低 / 最高价和产地（多个规格时取最低的低价、最高的高价，产地去重合并）。"""
    rs = [r for r in rows if r.get("prodName") == name and r.get("unitInfo") == "斤"
          and (spec is None or (r.get("specInfo") or "") == spec) and (num(r.get("avgPrice")) or 0) > 0]
    if not rs:
        return None
    day = max(r["pubDate"][:10] for r in rs)
    rs = [r for r in rs if r["pubDate"][:10] == day]
    lows = [v for v in (num(r.get("lowPrice")) for r in rs) if v and v > 0]
    highs = [v for v in (num(r.get("highPrice")) for r in rs) if v and v > 0]
    places = []
    for r in rs:
        for c in (r.get("place") or "").strip():
            if c not in places and c.strip() and c not in "\\/、,，":
                places.append(c)
    return {"date": day, "low": min(lows) if lows else None, "high": max(highs) if highs else None, "place": "".join(places)}

def xinfadi(prev_hist):
    hist = {k: dict(v) for k, v in prev_hist.items()}
    # 每天查最近 7 天的全部报价（补上漏抓的日子），新品种单独回补 90 天
    recent = xfd_query(TODAY - timedelta(days=7), TODAY)
    if not recent:
        raise ValueError("no rows in last 7 days")
    jobs = [b for b in BASKET if len(hist.get(b[0], {})) < 20]
    for b in jobs:                       # 逐个查、间隔 1 秒，避免触发限流
        try:
            rows = xfd_query(TODAY - timedelta(days=BACKFILL_DAYS), TODAY, b[3])
            hist.setdefault(b[0], {}).update(pick(rows, b[3], b[4]))
        except Exception as e:
            print(f"    回补 {b[1]} 失败：{e}")
        time.sleep(1)
    meta = {}
    for b in BASKET:
        hist.setdefault(b[0], {}).update(pick(recent, b[3], b[4]))
        m = pick_meta(recent, b[3], b[4])
        if m:
            meta[b[0]] = m
    cut = (TODAY - timedelta(days=KEEP_DAYS)).isoformat()
    for k in hist:
        hist[k] = dict(sorted((d, p) for d, p in hist[k].items() if d >= cut))
    xinfadi.backfilled = len(jobs)
    xinfadi.meta = meta
    return hist
xinfadi.backfilled = 0
xinfadi.meta = {}

# ── 农业农村部 ────────────────────────────────────────────────────────────────

def moa_index():
    d = json.loads(fetch(MOA + "/pi-info-day/getIndexByLevel", b"", {"Content-Type": "application/json"}))
    out = []
    for grp in d.get("content") or []:
        for x in grp:
            out.append({"name": x["indexName"], "date": x["publishDate"][:10], "value": x["indexValue"], "prev": x["nextRecordValue"]})
    if not out:
        raise ValueError("empty")
    return out

def moa_rank():
    d = json.loads(fetch(MOA + "/index/growthRanking", b"", {"Content-Type": "application/json"}))["data"]
    out = []
    for i, n in enumerate(d["names"]):
        out.append({"name": n, "price": num(d["avgPrice"][i]), "last": num(d["lastAvgPrice"][i]), "chg": num(d["priceHbs"][i]),
                    "unit": d["meteringUnit"][i] if isinstance(d.get("meteringUnit"), list) else "元/公斤", "date": d["date"][i]})
    if not out:
        raise ValueError("empty")
    return out

# ── 国家统计局 CPI ────────────────────────────────────────────────────────────

def cpi_links(pages):
    links = {}
    for i in range(pages):
        url = STATS + ("index.html" if i == 0 else f"index_{i}.html")
        try:
            s = fetch(url).decode("utf-8", "ignore")
        except Exception:
            break
        for href, title in re.findall(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', s, re.S):
            t = clean(title)
            m = re.match(r"(20\d\d)年(\d{1,2})月份居民消费价格", t)
            if m:
                links[f"{m[1]}-{int(m[2]):02d}"] = urllib.parse.urljoin(url, href)
    return links

def cpi_month(url):
    s = fetch(url).decode("utf-8", "ignore")
    out = {}
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", s, re.S):
        c = [re.sub(r"\s+", "", clean(x)) for x in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
        if len(c) >= 3 and c[0] in CPI_ROWS and CPI_ROWS[c[0]] not in out:
            mom, yoy = num(c[1]), num(c[2])
            if mom is not None and yoy is not None:
                out[CPI_ROWS[c[0]]] = {"mom": mom, "yoy": yoy}
    if "cpi" not in out:
        raise ValueError("table not found")
    return out

def cpi(prev):
    have = dict(prev)
    pages = 3 if len(have) >= CPI_MONTHS - 2 else 40     # 首次运行往前翻，之后只看前几页
    links = cpi_links(pages)
    if not links:
        raise ValueError("no CPI release links")
    todo = [(m, u) for m, u in sorted(links.items(), reverse=True)[:CPI_MONTHS] if m not in have]
    def one(job):
        m, u = job
        try:
            return m, {**cpi_month(u), "url": u}
        except Exception as e:
            print(f"    CPI {m} 解析失败：{e}")
            return m, None
    with ThreadPoolExecutor(4) as ex:
        for m, v in ex.map(one, todo):
            if v:
                have[m] = v
    return dict(sorted(have.items())[-36:])

# ── 主流程 ────────────────────────────────────────────────────────────────────

def main():
    prev = {}
    if OUT.exists():
        try:
            prev = json.loads(OUT.read_text("utf-8"))
        except Exception:
            prev = {}

    print(f"菜价物价 · {TODAY}")
    with ThreadPoolExecutor(4) as ex:
        f_x = ex.submit(run, "新发地", lambda: xinfadi(prev.get("xinfadi") or {}))
        f_i = ex.submit(run, "全国指数", moa_index)
        f_r = ex.submit(run, "全国涨跌榜", moa_rank)
        f_c = ex.submit(run, "CPI", lambda: cpi(prev.get("cpi") or {}))
    res = {"xinfadi": f_x.result(), "index": f_i.result(), "rank": f_r.result(), "cpi": f_c.result()}
    print(f"    新发地回补：{xinfadi.backfilled} 个品种")

    sources, val = {}, {}
    for k, (data, err) in res.items():
        stale = data is None
        old = prev.get("moa_index_today") if k == "index" else prev.get(k)
        val[k] = old if stale else data
        sources[k] = {"ok": not stale, "stale": stale and bool(old), "error": err}

    # 全国指数：今天的快照 + 每天累积的历史（接口只给当天和前一天）
    idx_hist = {k: dict(v) for k, v in (prev.get("moa_index_hist") or {}).items()}
    for x in val["index"] or []:
        h = idx_hist.setdefault(x["name"], {})
        h[x["date"]] = x["value"]
        if x.get("prev") is not None:
            prev_day = (date.fromisoformat(x["date"]) - timedelta(days=1)).isoformat()
            h.setdefault(prev_day, x["prev"])
    cut = (TODAY - timedelta(days=KEEP_DAYS)).isoformat()
    idx_hist = {k: dict(sorted((d, v) for d, v in h.items() if d >= cut)) for k, h in idx_hist.items()}

    # 全国涨跌榜：同样按天累积每个上榜品种的均价（元/斤，接口只给当天和前一天）
    rank_hist = {k: dict(v) for k, v in (prev.get("rank_hist") or {}).items()}
    for x in val["rank"] or []:
        k = 0.5 if x.get("unit") == "元/公斤" else 1
        h = rank_hist.setdefault(x["name"], {})
        if x.get("price") is not None and x.get("date"):
            h[x["date"]] = round(x["price"] * k, 3)
            if x.get("last") is not None:
                h.setdefault((date.fromisoformat(x["date"]) - timedelta(days=1)).isoformat(), round(x["last"] * k, 3))
    rank_hist = {k: dict(sorted((d, v) for d, v in h.items() if d >= cut)) for k, h in rank_hist.items()}

    # 新发地最近报价日的高低价、产地：本次没抓到的品种沿用上次
    xfd_meta = {**(prev.get("xinfadi_meta") or {}), **xinfadi.meta}

    data = {
        "updated": NOW.isoformat(timespec="seconds"),
        "today": TODAY.isoformat(),
        "sources": sources,
        "basket": [{"id": b[0], "name": b[1], "group": b[2], "xfd": b[3], "spec": b[4] or ""} for b in BASKET],
        "xinfadi": val["xinfadi"] or {},
        "xinfadi_meta": xfd_meta,
        "moa_index_today": val["index"] or [],
        "moa_index_hist": idx_hist,
        "rank": val["rank"] or [],
        "rank_hist": rank_hist,
        "cpi": val["cpi"] or {},
    }
    OUT.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), "utf-8")
    print(f"→ data.json {OUT.stat().st_size / 1024:.0f} KB")

if __name__ == "__main__":
    main()
