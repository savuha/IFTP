#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ИФТП · Парсер госзакупок ЕИС БЕЗ API-ключа
============================================
Парсит публичные страницы zakupki.gov.ru (расширенный поиск + карточки извещений),
отбирает закупки, соответствующие продукции АО «ИФТП» (Дубна), и пишет
iftp_zakupki_data.json рядом с дашбордом iftp_zakupki_dashboard.html.

Запуск: два раза в день — 9:00 и 15:00 по Москве (cron / планировщик задач Windows).

Зависимости:  pip install requests beautifulsoup4 lxml

Особенности:
- Работает без регистрации и токена (публичный раздел ЕИС).
- Кэш parser_state.json: повторные запуски докачивают только новые/устаревшие
  карточки, а не все подряд.
- Вежливые паузы между запросами + ретраи.
- --debug сохраняет сырые HTML-страницы в ./debug для подстройки селекторов.
"""

import os, re, sys, json, time, random, logging, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ----------------------------------------------------------------------------
# НАСТРОЙКИ
# ----------------------------------------------------------------------------
BASE     = "https://zakupki.gov.ru"
SEARCH   = BASE + "/epz/order/extendedsearch/results.html"
HERE     = Path(__file__).resolve().parent
OUT_FILE = HERE / "iftp_zakupki_data.json"
STATE    = HERE / "parser_state.json"
DEBUGDIR = HERE / "debug"

MSK  = timezone(timedelta(hours=3))
NOW  = lambda: datetime.now(MSK)
UA   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9"}

logging.basicConfig(filename=str(HERE / "parser.log"), level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("iftp-parser")

PAUSE     = (0.6, 1.4)   # пауза между запросами, сек
MAX_PAGES = 6            # страниц поиска на термин (по 50 извещений) — показываем ВСЕ подходящие
CACHE_TTL = 20           # часов: через сколько обновлять карточку в кэше
REQ_TIMEOUT = 25         # сек на попытку (было 40 — на блокирующем IP это душило job на часы)
RETRIES = 2              # попыток на запрос (было 3)

# ----------------------------------------------------------------------------
# СЛОВАРЬ ПРОДУКЦИИ ИФТП (ключи категорий = чипы на дашборде)
# ----------------------------------------------------------------------------
DICT = {
  "Полупроводниковые детекторы (Si, Ge, CdTe, алмазные)":
    ["полупроводников* детектор","планарн* детектор","кремниев* детектор","германиев* детектор",
     "HPGe","Ge(Li)","Si(Li)","теллурид* кадми*","CdTe","CdZnTe","алмазн* детектор",
     "ионизационн* камер*","детектор* альфа","детектор* бета"],
  "Сцинтилляторы и сцинтилляционные детекторы":
    ["сцинтиллятор","сцинтилляционн* детектор","сцинтилляционн* пластин*","пластмассов* сцинтиллятор",
     "СПС-Н","ПС-Б2","ПС-Б3","ПС-Н2","ПС-Н3","лить* сцинтиллятор","NaI(Tl)","CsI(Tl)","органическ* сцинтиллятор"],
  "Спектрометры и анализаторы излучений":
    ["спектрометр","спектрометрическ*","анализатор* состав* веществ*","рентгеновск* спектрометр",
     "гамма-спектрометр","альфа-спектрометр","бета-спектрометр","рентгенофлуоресцентн* анализатор",
     "многоканальн* анализатор","анализатор* рентгеновск* излучен*"],
  "Радиометры и дозиметры":
    ["дозиметр","радиометр","измерител* мощности дозы","ИМД","монитор* радиационн*",
     "сигнализатор* радиац*","средств* радиационн* контрол*","РГА-1К",
     "контрольно-измерительн* прибор* радиац*","радиометрическ* контрол*"],
  "Радиоизотопные приборы (толщиномеры, плотномеры, уровнемеры)":
    ["толщиномер","плотномер","уровнемер","влагомер","зольномер","релейн* прибор",
     "радиоизотопн* прибор","радиоизотопн* контрол*","бесконтактн* измер* уровн*",
     "бесконтактн* измер* плотност*","бесконтактн* измер* влажност*"],
  "Блоки гамма-излучения и радионуклидные источники":
    ["блок* гамма-излучен*","БГИ","источник* ионизирующ* излучен*","ЗРнИ",
     "закрыт* радионуклидн* источник*","цезий-137","Cs-137","кобальт-60","Co-60",
     "америций-241","Am-241","заряд* блок* источник*"],
  "Пожарные извещатели (ионизационные)":
    ["извещател* пожарн* ионизацион*","ионизационн* дымов* извещател*","извещател* пожарн*"],
  "Услуги: поверка, ТО, монтаж":
    ["поверк* дозиметр*","поверк* радиометр*","поверк* прибор* радиац*","калибровк* радиац*",
     "техническ* обслуживан* радиоизотопн*","техническ* обслуживан* прибор* радиац*",
     "монтаж* радиоизотопн*","ремонт* детектор*","градуировк* радиоизотопн*"],
}
NEGATIVE = ["канцеляр","мебель","текстиль","обувь","продукт питан","бензин","дизель",
            "газель","автомобил","хозяйственн* товар*","спортивн* инвентар*","игруш*"]

# Поисковые запросы для ЕИС
SEARCH_TERMS = sorted({
    "сцинтиллятор", "сцинтилляционный детектор", "сцинтилляционный блок",
    "полупроводниковый детектор", "германиевый детектор", "HPGe",
    "CdTe", "теллурид кадмия", "алмазный детектор", "ионизационная камера",
    "спектрометр гамма", "альфа-спектрометр", "рентгеновский спектрометр",
    "дозиметр", "радиометр", "радиационный контроль", "радиационный монитор",
    "измеритель мощности дозы", "сигнализатор радиации",
    "толщиномер", "плотномер", "уровнемер", "влагомер", "зольномер",
    "радиоизотопный прибор", "радиоизотопный контроль",
    "блок гамма-излучения", "радионуклидный источник", "цезий-137", "кобальт-60",
    "извещатель пожарный ионизационный",
    "поверка дозиметр", "калибровка радиометр", "техническое обслуживание радиоизотоп",
})

# ----------------------------------------------------------------------------
# УТИЛИТЫ
# ----------------------------------------------------------------------------
def esc(w):
    return re.escape(w).replace(r"\*", "[а-яa-z0-9-]*").replace(r"\?", ".?")

def match_categories(text):
    t = (text or "").lower()
    neg = any(re.search(esc(w.lower()), t) for w in NEGATIVE)
    out = []
    for cat, kws in DICT.items():
        hits = [w for w in kws if re.search(esc(w.lower()), t)]
        if hits:
            out.append({"cat": cat, "hits": hits,
                        "score": max(10, min(100, len(hits) * 34 - (15 if neg else 0)))})
    return out

def parse_money(s):
    if not s: return None
    s = re.sub(r"[^\d,.\s\u00a0]", "", s).replace("\u00a0", "").replace(" ", "").replace(",", ".")
    try: return float(s)
    except ValueError: return None

def ru_dt(s):
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2}))?", s or "")
    if not m: return None
    d, mo, y, h, mi = m.groups()
    return f"{y}-{mo}-{d}T{(h or '00')}:{(mi or '00')}:00+03:00"

# ----------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------
sess = requests.Session(); sess.headers.update(UA)

NET_ERRORS = 0  # счётчик сетевых сбоев подряд — сигнал блокировки/недоступности сайта

def get(url, **kw):
    global NET_ERRORS
    for attempt in range(RETRIES):
        try:
            r = sess.get(url, timeout=REQ_TIMEOUT, **kw)
            if r.status_code == 200:
                r.encoding = "utf-8"
                NET_ERRORS = 0
                return r.text
            msg = f"HTTP {r.status_code} {url}"
            log.warning(msg); print("  ! " + msg, flush=True)
        except requests.RequestException as e:
            msg = f"сетевая ошибка ({e.__class__.__name__}: {e}) {url}"
            log.warning(msg); print("  ! " + msg, flush=True)
            NET_ERRORS += 1
        time.sleep(random.uniform(2, 5) * (attempt + 1))
    return None

def polite():
    time.sleep(random.uniform(*PAUSE))

# ----------------------------------------------------------------------------
# 1) ПОИСК по расширенному поиску ЕИС (публичный, без ключа)
# ----------------------------------------------------------------------------
def search_notices(term, debug=False):
    found = {}
    for page in range(1, MAX_PAGES + 1):
        params = {
            "searchString": term, "morphology": "on",
            "search-filter": "Дате размещения", "sortBy": "UPDATE_DATE",
            "sortDirection": "false", "recordsPerPage": "_50",
            "pageNumber": str(page),  # реальное имя параметра пагинации на zakupki.gov.ru
            "fz44": "on", "fz223": "on",
            "af": "on", "ca": "on", "pa": "on",
            "currencyIdGeneral": "-1",
            "applSubmissionCloseDateFrom": NOW().strftime("%d.%m.%Y"),
        }
        html = get(SEARCH, params=params)
        if not html: break
        if debug and page == 1:
            DEBUGDIR.mkdir(exist_ok=True)
            (DEBUGDIR / ("search_" + re.sub(r"\W", "_", term) + ".html")).write_text(html, "utf-8")
        soup = BeautifulSoup(html, "lxml")
        n = 0
        for a in soup.find_all("a", href=True):
            m = re.search(r"regNumber=(\d+)", a["href"])
            if m:
                href = urljoin(BASE, a["href"])
                found.setdefault(m.group(1), href)
                n += 1
        if n < 20:  # страница неполная — дальше листать нечего
            break
        polite()
    return found

# ----------------------------------------------------------------------------
# 2) КАРТОЧКА ИЗВЕЩЕНИЯ: common-info (ea20/ea44/223)
# ----------------------------------------------------------------------------
def _after_label(txt, labels, val_re, window=140):
    for lab in labels:
        m = re.search(lab + r".{0," + str(window) + r"}?" + val_re, txt, re.S | re.I)
        if m: return m.group(1)
    return None

def parse_notice(number, url, debug=False):
    html = get(url)
    if not html: return None
    if debug:
        DEBUGDIR.mkdir(exist_ok=True)
        (DEBUGDIR / ("notice_" + number + ".html")).write_text(html, "utf-8")
    soup = BeautifulSoup(html, "lxml")
    txt = soup.get_text(" ", strip=True)

    # предмет: og:title или <title> ("№ … — Предмет | zakupki")
    subject = None
    og = soup.find("meta", property="og:title")
    if og and og.get("content"): subject = og["content"].strip()
    if not subject and soup.title:
        subject = soup.title.get_text(" ", strip=True)
    if subject:
        for junk in ("| zakupki.gov.ru", "zakupki.gov.ru"):
            subject = subject.replace(junk, "")
        subject = re.sub(r"^\s*№?\s*" + re.escape(number) + r"\s*[—–-]?\s*", "", subject)
        subject = subject.strip(" —–-|")

    customer = ""
    for a in soup.find_all("a", href=True):
        if "organization" in a["href"] or "customer" in a["href"].lower():
            t = a.get_text(" ", strip=True)
            if len(t) > 5 and not customer:
                customer = t
    if not customer:
        customer = _after_label(txt, [r"Заказчик", r"Наименование организации"],
                                r"([А-ЯЁA-Z][\wЁё«»\"'.,\- ]{8,140})") or ""

    amount = parse_money(_after_label(txt,
        [r"Начальная \(максимальная\) цена контракта", r"Начальная цена контракта",
         r"Максимальное значение цены контракта", r"Цена контракта"],
        r"((?:\d[\d\s\u00a0.,]*){1,3})"))

    deadline = ru_dt(_after_label(txt,
        [r"Окончание подачи заявок", r"Дата и время окончания срока подачи заявок",
         r"Дата окончания подачи заявок", r"Окончание срока подачи заявок"],
        r"(\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?)") or "")

    published = ru_dt(_after_label(txt,
        [r"Дата публикации извещения", r"Дата размещения извещения"],
        r"(\d{2}\.\d{2}\.\d{4})") or "")

    stage = "Подача заявок"
    st = _after_label(txt, [r"Этап закупки", r"Стадия"], r"([А-ЯЁA-Z][\wЁё\s]{3,60})")
    if st: stage = st.strip()

    if not subject:
        return None
    return {"number": number, "link": url, "customer": customer.strip(), "region": "",
            "subject": subject.strip(), "amount": amount, "published": published,
            "deadline": deadline, "stage": stage, "isNew": False}

# ----------------------------------------------------------------------------
# ОСНОВНОЙ ЦИКЛ
# ----------------------------------------------------------------------------
def load_json(p, default):
    try: return json.loads(Path(p).read_text("utf-8"))
    except Exception: return default

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--debug", action="store_true", help="сохранять сырые HTML в ./debug")
    ap.add_argument("--limit", type=int, default=0, help="ограничить число новых карточек")
    args = ap.parse_args()

    state = load_json(STATE, {})
    old_out = load_json(OUT_FILE, {"items": []})
    prev_numbers = {i["number"] for i in old_out.get("items", [])}

    candidates = {}
    for i, term in enumerate(SEARCH_TERMS, 1):
        res = search_notices(term, debug=args.debug and i == 1)
        candidates.update(res)
        print(f"[{i}/{len(SEARCH_TERMS)}] '{term}' → всего найдено: {len(candidates)}", flush=True)
        if NET_ERRORS >= 6:
            msg = ("СТОП: 6 сетевых сбоев подряд к zakupki.gov.ru. Похоже, сайт недоступен "
                   "с этого IP (геоблокировка/бан) или сменилась разметка/URL поиска. "
                   "Запустите на РФ-хосте с --debug и проверьте debug/*.html.")
            log.error(msg); print("!!! " + msg, flush=True)
            sys.exit(1)
        polite()

    fresh, need = {}, []
    for num, url in candidates.items():
        c = state.get(num)
        if c and (NOW() - datetime.fromisoformat(c["fetched_at"])) < timedelta(hours=CACHE_TTL):
            fresh[num] = c["data"]
        else:
            need.append((num, url))
    if args.limit: need = need[:args.limit]
    print(f"Новых/устаревших карточек к скачиванию: {len(need)} (из кэша: {len(fresh)})", flush=True)

    for j, (num, url) in enumerate(need, 1):
        rec = parse_notice(num, url, debug=args.debug)
        if rec:
            state[num] = {"fetched_at": NOW().isoformat(), "data": rec}
        else:
            log.warning("не распарсилось: %s %s", num, url)
        if j % 10 == 0:
            STATE.write_text(json.dumps(state, ensure_ascii=False), "utf-8")
        print(f"\r  карточки {j}/{len(need)}", end="", flush=True)
        polite()
    print()
    STATE.write_text(json.dumps(state, ensure_ascii=False), "utf-8")

    items = []
    for num in candidates:
        rec = state.get(num, {}).get("data")
        if not rec:
            continue
        cats = match_categories(rec.get("subject", "") + " " + rec.get("customer", ""))
        if not cats:
            continue
        dl = rec.get("deadline")
        if dl:
            try:
                if datetime.fromisoformat(dl) < NOW() - timedelta(hours=3):
                    continue                    # дедлайн прошёл — пропускаем
            except ValueError:
                pass
        is_new = num not in prev_numbers
        pub = rec.get("published")
        if pub and is_new:
            try:
                is_new = (NOW() - datetime.fromisoformat(pub)) < timedelta(hours=36)
            except ValueError:
                pass
        rec["isNew"] = is_new
        rec["cats"] = cats
        items.append(rec)

    items.sort(key=lambda x: (x.get("deadline") or "9999", -(x.get("amount") or 0)))
    outj = {"generated_at": NOW().isoformat(), "source": "zakupki.gov.ru (парсер)",
            "count": len(items), "dict": DICT, "items": items}
    OUT_FILE.write_text(json.dumps(outj, ensure_ascii=False, indent=1), "utf-8")
    total = sum(i.get("amount") or 0 for i in items)
    log.info("OK: %d закупок, сумма %.0f RUB", len(items), total)
    print(f"OK: {len(items)} подходящих закупок, сумма НМЦК {total:,.0f} RUB -> {OUT_FILE}")

if __name__ == "__main__":
    main()
