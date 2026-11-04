"""Жарыялардын баасы жана бекер лимит.

Эреже: бир байланышка (телефон / WhatsApp / Telegram) алгачкы FREE_LIMIT
акылуу категориядагы жарыя бекер. Андан кийин категорияга жараша төлөнөт.
PAY_INFO (реквизиттер) коюлбаса, төлөм өчүк: бардык жарыялар бекер,
бирок эсеп жүргүзүлө берет.
"""
import base64
import re

FREE_LIMIT = 10

# Деңгээлдер (₽)
FREE_CATS = {"help", "jobseek"}
TIER_200 = {"law", "fin", "ticket", "med", "auto", "carsale"}
TIER_100 = {"job", "house", "cargo", "ride", "food", "beauty", "edu", "repair"}
TIER_50 = {"market"}

# Категориянын ичиндеги арзаныраак түрлөр (жеке адамдардын бир жолку жарыялары)
SUB_PRICE = {
    ("market", "Бекер берем"): 0,
    ("ride", "Кыргызстанга кайтам"): 50,
    ("ride", "Россияга барам"): 50,
    ("house", "Батир / койка издейм"): 50,
    ("carsale", "Сатып алам"): 50,
    ("carsale", "Ижарага алам"): 50,
}


def cat_price(cat: str) -> int:
    if cat in FREE_CATS:
        return 0
    if cat in TIER_200:
        return 200
    if cat in TIER_100:
        return 100
    if cat in TIER_50:
        return 50
    return 100


SEP = " · "  # бир нече түр ушул белги менен бириктирилет (түрлөрдүн атында үтүр бар)


def split_subs(sub: str) -> list:
    sub = sub or ""
    parts = sub.split(SEP) if SEP in sub else sub.split(", ")
    return [x.strip() for x in parts if x.strip()]


def fee_for(cat: str, sub: str) -> int:
    """Бир нече түр тандалса, эң кымбаты эсептелет."""
    base = cat_price(cat)
    subs = split_subs(sub)
    if not subs:
        return base
    # «Нан, боорсок, торт» сыяктуу аттар үтүр менен бөлүнүп калса — алар SUB_PRICE'та жок,
    # ошондуктан категориянын негизги баасы колдонулат.
    return max(SUB_PRICE.get((cat, x), base) for x in subs)


def contact_key(phone: str, wa: str, tg: str) -> str:
    """Бекер лимитти эсептөө үчүн байланышты бир түргө келтирет."""
    for raw in (phone, wa):
        d = re.sub(r"\D", "", raw or "")
        if len(d) >= 9:
            if len(d) == 11 and d[0] == "8":  # 8 900 … → 7 900 …
                d = "7" + d[1:]
            if len(d) == 10 and d[0] == "9":  # 900 … (кодсуз Россия)
                d = "7" + d
            return "p:" + d
    t = (tg or "").strip().lower().lstrip("@")
    t = re.sub(r"^(https?://)?(t\.me/)", "", t)
    if t:
        return "t:" + t
    return ""


MAX_RECEIPT_BYTES = 1_200_000


def parse_receipt(data_url: str):
    """'data:image/jpeg;base64,...' → (bytes, mime). Ката болсо ValueError."""
    m = re.match(r"^data:(image/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=\s]+)$", data_url or "")
    if not m:
        raise ValueError("Чектин сүрөтү туура эмес. JPG же PNG сүрөт жүктөңүз.")
    raw = base64.b64decode(m.group(2), validate=False)
    if len(raw) > MAX_RECEIPT_BYTES:
        raise ValueError("Чектин сүрөтү өтө чоң. Скриншотту кайра жүктөп көрүңүз.")
    if len(raw) < 1000:
        raise ValueError("Чектин сүрөтү бош. Кайра тандаңыз.")
    return raw, m.group(1)
