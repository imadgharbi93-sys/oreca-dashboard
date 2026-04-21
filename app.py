import json
import os
import re
import unicodedata
import uuid
import zipfile
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

from flask import Flask, flash, jsonify, make_response, redirect, render_template, request, url_for
from flask_login import LoginManager, login_required, login_user, logout_user, current_user, UserMixin

from data.restaurants import RESTAURANTS
from data.users import USERS

app = Flask(__name__)
app.secret_key = "oreca_secret_key_2025"

# ── Flask-Login ────────────────────────────────────────────────────────────────

class User(UserMixin):
    def __init__(self, id):
        self.id = id

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

@login_manager.user_loader
def load_user(user_id):
    if user_id in USERS:
        return User(user_id)
    return None

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "ca_data.json")

MONTHS = [
    "2026-01", "2026-02", "2026-03", "2026-04",
    "2026-05", "2026-06", "2026-07", "2026-08",
    "2026-09", "2026-10", "2026-11", "2026-12",
]
MONTH_LABELS = [
    "Jan 2026", "Fév 2026", "Mar 2026", "Avr 2026",
    "Mai 2026", "Juin 2026", "Juil 2026", "Août 2026",
    "Sep 2026", "Oct 2026", "Nov 2026", "Déc 2026",
]
MONTHS_2025 = [
    "2025-01", "2025-02", "2025-03", "2025-04",
    "2025-05", "2025-06", "2025-07", "2025-08",
    "2025-09", "2025-10", "2025-11", "2025-12",
]
MONTH_LABELS_2025 = [
    "Jan 2025", "Fév 2025", "Mar 2025", "Avr 2025",
    "Mai 2025", "Juin 2025", "Juil 2025", "Août 2025",
    "Sep 2025", "Oct 2025", "Nov 2025", "Déc 2025",
]
# Tous les mois (2025 + 2026) — utilisé pour saisie et profil restaurant
ALL_MONTHS       = MONTHS_2025 + MONTHS
ALL_MONTH_LABELS = MONTH_LABELS_2025 + MONTH_LABELS

BRAND_COLORS = {
    "Chamas Tacos":  "#E28F0A",
    "O'Cheese":      "#D82E2E",
    "Delys Station": "#DB29C9",
}

PRODUCT_BRAND_COLORS = {
    "Chamas Tacos":"#E28F0A",
    "O'Cheese": "#D82E2E",
    "Delys Station": "#DB29C9",
}


# ── Context processor ─────────────────────────────────────────────────────────

@app.context_processor
def inject_globals():
    """Rend RESTAURANTS et USERS disponibles dans tous les templates."""
    return {"restaurants": RESTAURANTS, "USERS": USERS}


# ── Jinja2 filter ──────────────────────────────────────────────────────────────

@app.template_filter("fr_number")
def fr_number(value):
    try:
        return "{:,.0f}".format(float(value)).replace(",", "\u202f")
    except (TypeError, ValueError):
        return str(value)


# ── Data helpers ───────────────────────────────────────────────────────────────

def _generate_empty_data():
    """Données vierges — toutes les valeurs à 0."""
    return {r["id"]: {m: 0 for m in MONTHS} for r in RESTAURANTS}


def load_data():
    if not os.path.exists(DATA_FILE):
        data = _generate_empty_data()
        _save_data(data)
        return data
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _get_all_restaurants(data):
    """Retourne les restaurants statiques + ceux ajoutés dynamiquement."""
    custom = data.get("custom_restaurants", [])
    return list(RESTAURANTS) + [r for r in custom if isinstance(r, dict) and "id" in r]


def _normalize_text(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _slugify_text(value):
    return _normalize_text(value).replace(" ", "-")


def _parse_excel_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()")
    text = text.replace("\u202f", "").replace("\xa0", "").replace("€", "").replace(" ", "")

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        number = float(text)
    except ValueError:
        return None
    return -number if negative else number


def _xlsx_column_index(cell_ref):
    letters = "".join(ch for ch in str(cell_ref or "") if ch.isalpha()).upper()
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - 64)
    return idx - 1 if idx else 0


def _xlsx_read_rows(file_storage):
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_ns = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}

    file_storage.stream.seek(0)
    with zipfile.ZipFile(file_storage.stream) as zf:
        shared_strings = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("main:si", ns):
                parts = []
                for node in si.iterfind(".//main:t", ns):
                    parts.append(node.text or "")
                shared_strings.append("".join(parts))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = workbook.find("main:sheets", ns)
        if sheets is None or not list(sheets):
            return []
        first_sheet = list(sheets)[0]
        rel_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")

        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels.findall("rel:Relationship", rel_ns):
            if rel.attrib.get("Id") == rel_id:
                target = rel.attrib.get("Target")
                break
        if not target:
            return []

        clean_target = target.lstrip("/")
        sheet_path = clean_target if clean_target.startswith("xl/") else f"xl/{clean_target}"
        sheet_root = ET.fromstring(zf.read(sheet_path))
        rows = []
        for row in sheet_root.findall(".//main:sheetData/main:row", ns):
            values = []
            current_col = 0
            for cell in row.findall("main:c", ns):
                col_idx = _xlsx_column_index(cell.attrib.get("r"))
                while current_col < col_idx:
                    values.append("")
                    current_col += 1

                cell_type = cell.attrib.get("t")
                raw_value = ""
                if cell_type == "inlineStr":
                    raw_value = "".join(
                        node.text or "" for node in cell.findall(".//main:t", ns)
                    )
                else:
                    value_node = cell.find("main:v", ns)
                    raw_value = value_node.text if value_node is not None else ""
                    if cell_type == "s" and raw_value != "":
                        try:
                            raw_value = shared_strings[int(raw_value)]
                        except (ValueError, IndexError):
                            pass
                values.append(raw_value)
                current_col += 1
            rows.append(values)
        return rows


def _detect_product_columns(headers):
    normalized = [_normalize_text(h) for h in headers]

    def find_index(*keywords):
        for idx, header in enumerate(normalized):
            if any(keyword in header for keyword in keywords):
                return idx
        return None

    sales_idx = None
    if len(headers) > 5:
        # Le fichier métier place le "Total" en colonne F.
        sales_idx = 5
    if sales_idx is None:
        sales_idx = find_index("total", "vente", "ventes", "montant", "ca", "net")

    return {
        "restaurant": find_index("restaurant", "resto", "point de vente", "magasin", "site", "etablissement"),
        "category": find_index("categorie", "category", "famille", "groupe", "rayon"),
        "product": find_index("article", "produit", "item", "designation", "libelle"),
        "sales": sales_idx,
        "quantity": 1 if len(headers) > 1 else find_index("quantite vendue", "qte vendue", "qty vendue", "nb vendu", "quantite", "quantity", "qte", "qty", "nb ventes", "nombre ventes", "nombre", "vendu", "unites"),
        "unit_price": find_index("prix unitaire", "prix unit", "pu ", "tarif"),
    }


_MANUAL_ALIASES = {
    "O'Cheese Guillemins": {"nab srl", "nab srl o cheese liege", "nab srl ocheese liege", "guillemens", "guillemins"},
    "O'Cheese Kennedy":    {"o cheese burger", "ocheese kennedy", "kennedy"},
    "O'Cheese Jambes":     {"jane wane", "ocheese jambes"},
    "O'Cheese Rogier":     {"coopsa rogier", "ocheese rogier"},
    "O'Cheese Anderlecht": {"eas", "ocheese anderlecht"},
    "Delys 1 Anspach":     {"delys station debrouchere", "delys station debroukere", "delys station anspach", "debrouchere", "debroukere"},
    "Delys 2 Rue Neuve":   {"delys station rue neuve", "rue neuve"},
}

def _build_restaurant_aliases(restaurants):
    aliases = []
    for resto in restaurants:
        name = resto["name"]
        nom_juridique = resto.get("nom_juridique", "")
        candidates = {
            _normalize_text(name),
            _normalize_text(name.replace("Chamas Tacos ", "")),
            _normalize_text(name.replace("O'Cheese ", "")),
            _normalize_text(name.replace("Delys Station ", "")),
            _normalize_text(name.replace("Chamas ", "")),
            _normalize_text(name.replace("Delys ", "")),
        }
        # Ajoute le nom juridique comme alias
        if nom_juridique:
            candidates.add(_normalize_text(nom_juridique))
        # Ajoute les aliases manuels
        candidates.update(
            _normalize_text(alias)
            for alias in _MANUAL_ALIASES.get(name, set())
        )
        aliases.append((resto, {c for c in candidates if c}))
    return aliases


def _match_restaurant_name(raw_name, aliases):
    target = _normalize_text(raw_name)
    if not target:
        return None

    for resto, names in aliases:
        if target in names:
            return resto
    for resto, names in aliases:
        if any(target in name or name in target for name in names):
            return resto
    return None


_EXCLUDED_PRODUCT_PATTERNS = [
    # Matériel / technique
    r"^bipper",
    r"\bbipper\b",
    # Modificateurs / options / suppléments négatifs
    r"^sans\b",
    r"^avec\b",
    r"\bsans\s+\w+",
    r"^extra\b",
    r"^ajout\b",
    r"^retrait\b",
    r"^modif",
    r"^option\b",
    r"^supplement\b",
    r"^suppl[e\.]",
    # Remises / frais
    r"^remise\b",
    r"^reduction\b",
    r"^frais\b",
    r"^livraison\b",
    r"^pourboire\b",
]
_EXCLUDED_PRODUCT_RE = re.compile(
    "|".join(_EXCLUDED_PRODUCT_PATTERNS), re.IGNORECASE
)


def _is_excluded_product(name):
    return bool(_EXCLUDED_PRODUCT_RE.search(_normalize_text(name)))


def _import_product_sales_xlsx(file_storage, selected_brand, selected_month, restaurants):
    rows = _xlsx_read_rows(file_storage)
    if not rows:
        raise ValueError("Le fichier Excel est vide ou illisible.")

    header_idx = next((idx for idx, row in enumerate(rows) if any(str(cell).strip() for cell in row)), None)
    if header_idx is None:
        raise ValueError("Impossible de trouver l'en-tête du fichier.")

    headers = [str(cell).strip() for cell in rows[header_idx]]
    columns = _detect_product_columns(headers)
    missing = [label for label, idx in columns.items() if idx is None and label not in ("category", "quantity", "unit_price")]
    if missing:
        raise ValueError("Colonnes requises introuvables dans le fichier Excel.")

    # Noms réels des colonnes détectées (pour debug dans la preview)
    detected_headers = {
        label: (headers[idx] if idx is not None and idx < len(headers) else None)
        for label, idx in columns.items()
    }

    aliases = _build_restaurant_aliases(restaurants)
    aggregated = {}
    stats = {
        "rows_total": 0,
        "rows_kept": 0,
        "rows_negative_skipped": 0,
        "rows_zero_skipped": 0,
        "rows_unknown_restaurant": 0,
        "rows_missing_sales": 0,
        "rows_excluded_product": 0,
        "quantity_column_found": columns.get("quantity") is not None,
        "quantity_column_name": detected_headers.get("quantity"),
        "detected_headers": detected_headers,
    }

    for row in rows[header_idx + 1:]:
        if not any(str(cell).strip() for cell in row):
            continue
        stats["rows_total"] += 1

        restaurant_name = row[columns["restaurant"]] if columns["restaurant"] < len(row) else ""
        product_name = row[columns["product"]] if columns["product"] < len(row) else ""
        category_name = row[columns["category"]] if columns["category"] is not None and columns["category"] < len(row) else "Non classé"
        sales_value = row[columns["sales"]] if columns["sales"] < len(row) else ""

        qty_idx = columns.get("quantity")
        qty_value = row[qty_idx] if qty_idx is not None and qty_idx < len(row) else ""

        sales = _parse_excel_number(sales_value)
        quantite = _parse_excel_number(qty_value)

        if sales is None:
            stats["rows_missing_sales"] += 1
            continue

        if _is_excluded_product(product_name):
            stats["rows_excluded_product"] += 1
            continue

        resto = _match_restaurant_name(restaurant_name, aliases)
        if resto is None:
            if sales != 0 or (quantite and quantite != 0):
                stats["rows_unknown_restaurant"] += 1
            continue

        rid = resto["id"]
        article_key = _slugify_text(product_name) or f"article-{stats['rows_total']}"
        resto_bucket = aggregated.setdefault(rid, {})
        article_bucket = resto_bucket.setdefault(article_key, {
            "article": str(product_name).strip() or "Article sans nom",
            "categorie": str(category_name).strip() or "Non classé",
            "ventes": 0.0,
            "quantite": 0.0,
        })
        # On n'additionne que les lignes positives pour les deux champs.
        # Les lignes négatives sont des retraits cash → ignorées pour ne pas
        # fausser ni le CA ni les quantités (ex: Hamburger à -18133€)
        if sales > 0:
            article_bucket["ventes"] = round(article_bucket["ventes"] + sales, 2)
        if quantite is not None and quantite > 0:
            article_bucket["quantite"] = round(article_bucket["quantite"] + quantite, 2)
        stats["rows_total_raw"] = stats.get("rows_total_raw", 0) + 1

    # ── Filtrage post-agrégation ───────────────────────────────────────────────
    # On supprime les articles dont le total net est ≤ 0 après neutralisation
    # des lignes miroirs (fausses commandes pour retrait cash).
    clean_aggregated = {}
    for rid, articles in aggregated.items():
        for article_key, article in articles.items():
            q = article["quantite"]
            if q > 0 or article["ventes"] > 0:
                clean_aggregated.setdefault(rid, {})[article_key] = article
                stats["rows_kept"] += 1
            else:
                stats["rows_negative_skipped"] += 1

    return {
        "month": selected_month,
        "brand": selected_brand,
        "restaurants": clean_aggregated,
        "stats": stats,
        "headers_found": headers,
    }


def _product_sales_summary(data, month, brand):
    sales_root = data.get("ventes_produits", {}).get(month, {}).get(brand, {})
    imports_root = data.get("ventes_produits_imports", {}).get(month, {}).get(brand, {})

    restaurants = sales_root.get("restaurants", {}) if isinstance(sales_root, dict) else {}
    total_sales = 0
    product_count = 0
    for articles in restaurants.values():
        product_count += len(articles)
        for article in articles.values():
            total_sales += float(article.get("ventes", 0) or 0)

    return {
        "restaurant_count": len(restaurants),
        "product_count": product_count,
        "total_sales": round(total_sales, 2),
        "last_imported_at": imports_root.get("imported_at"),
        "last_filename": imports_root.get("filename"),
        "last_stats": imports_root.get("stats", {}),
    }


def _product_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _month_product_records(product_records, month, brand=None, restaurant_id=None):
    return [
        record for record in product_records
        if record["month"] == month
        and (brand in (None, "all") or record["brand"] == brand)
        and (restaurant_id in (None, "all") or record["restaurant_id"] == restaurant_id)
    ]


def _aggregate_product_rows(records, key_fields):
    aggregated = {}
    for record in records:
        key = tuple(record[field] for field in key_fields)
        bucket = aggregated.setdefault(key, {
            "quantite_totale": 0.0,
            "ca_total": 0.0,
            "weighted_price_sum": 0.0,
            "restaurants": set(),
            "brands": {},
        })
        bucket["quantite_totale"] += record["quantite"]
        bucket["ca_total"] += record["ca"]
        bucket["weighted_price_sum"] += record["prix_unitaire"] * record["quantite"]
        bucket["restaurants"].add(record["restaurant_id"])
        bucket["brands"][record["brand"]] = bucket["brands"].get(record["brand"], 0.0) + record["ca"]

    rows = []
    for key, bucket in aggregated.items():
        row = {field: key[idx] for idx, field in enumerate(key_fields)}
        qty = bucket["quantite_totale"]
        ca_total = bucket["ca_total"]
        row.update({
            "quantite_totale": round(qty, 2),
            "ca_total": round(ca_total, 2),
            "prix_unitaire_moyen": round(bucket["weighted_price_sum"] / qty, 2) if qty else 0.0,
            "restaurant_count": len(bucket["restaurants"]),
            "dominant_brand": max(bucket["brands"], key=bucket["brands"].get) if bucket["brands"] else None,
            "brands": {brand: round(val, 2) for brand, val in bucket["brands"].items()},
        })
        rows.append(row)
    return rows


def _build_products_summary(records, prev_records, total_ca_group, restaurants):
    restaurants_by_id = {r["id"]: r for r in restaurants}
    volume_total = round(sum(record["quantite"] for record in records), 2)
    ca_produits_total = round(sum(record["ca"] for record in records), 2)
    prix_moyen = round(ca_produits_total / volume_total, 2) if volume_total else 0.0

    prev_volume_total = round(sum(record["quantite"] for record in prev_records), 2)
    prev_ca_total = round(sum(record["ca"] for record in prev_records), 2)
    prev_prix_moyen = round(prev_ca_total / prev_volume_total, 2) if prev_volume_total else 0.0

    produits_agg = _aggregate_product_rows(records, ["nom"])
    prev_by_product = {
        row["nom"]: row for row in _aggregate_product_rows(prev_records, ["nom"])
    }

    produits_agg.sort(key=lambda item: item["ca_total"], reverse=True)
    for index, row in enumerate(produits_agg, start=1):
        prev_qty = prev_by_product.get(row["nom"], {}).get("quantite_totale")
        row["rank"] = index
        row["evolution_quantite_pct"] = (
            round((row["quantite_totale"] - prev_qty) / prev_qty * 100, 1)
            if prev_qty not in (None, 0) else None
        )
        dominant_brand = row.get("dominant_brand")
        row["marque"] = dominant_brand

    ca_total_by_brand = {}
    for record in records:
        ca_total_by_brand[record["brand"]] = ca_total_by_brand.get(record["brand"], 0.0) + record["ca"]

    produits_par_marque = {}
    produit_star_par_marque = {}
    repartition_marques = []
    for brand in PRODUCT_BRAND_COLORS:
        brand_records = [record for record in records if record["brand"] == brand]
        brand_products = _aggregate_product_rows(brand_records, ["nom"])
        brand_total = round(sum(item["ca_total"] for item in brand_products), 2)
        for row in brand_products:
            row["part_ca_marque_pct"] = round(row["ca_total"] / brand_total * 100, 1) if brand_total else 0.0
            row["marque"] = brand
        brand_products.sort(key=lambda item: item["ca_total"], reverse=True)
        produits_par_marque[brand] = brand_products
        produit_star_par_marque[brand] = brand_products[0] if brand_products else None
        repartition_marques.append({
            "brand": brand,
            "ca_total": brand_total,
            "quantity_total": round(sum(item["quantite_totale"] for item in brand_products), 2),
            "product_count": len(brand_products),
        })

    top5_produits_groupe = produits_agg[:5]
    top5_produits_volume = sorted(
        produits_agg,
        key=lambda item: item["quantite_totale"],
        reverse=True,
    )[:5]
    flop5_produits = sorted(
        [item for item in produits_agg if item["ca_total"] > 0],
        key=lambda item: item["ca_total"],
    )[:5]

    produits_par_restaurant = {}
    for resto in restaurants:
        resto_records = [record for record in records if record["restaurant_id"] == resto["id"]]
        if not resto_records:
            continue
        aggregated = _aggregate_product_rows(resto_records, ["nom"])
        aggregated.sort(key=lambda item: item["ca_total"], reverse=True)
        produits_par_restaurant[resto["id"]] = {
            "restaurant_id": resto["id"],
            "restaurant_name": resto["name"],
            "brand": resto["brand"],
            "top3": aggregated[:3],
            "top5": aggregated[:5],
            "nb_produits": len(aggregated),
            "ca_total_produits": round(sum(item["ca_total"] for item in aggregated), 2),
        }

    top_product = top5_produits_groupe[0] if top5_produits_groupe else None
    top_product_qty = top_product["quantite_totale"] if top_product else 0

    return {
        "top5_produits_groupe": top5_produits_groupe,
        "top5_produits_volume": top5_produits_volume,
        "flop5_produits": flop5_produits,
        "produit_star_par_marque": produit_star_par_marque,
        "nb_produits_actifs": len(produits_agg),
        "ca_produits_total": ca_produits_total,
        "volume_total_vendu": volume_total,
        "volume_total_evolution_pct": round((volume_total - prev_volume_total) / prev_volume_total * 100, 1) if prev_volume_total else None,
        "prix_moyen_article": prix_moyen,
        "prix_moyen_evolution_pct": round((prix_moyen - prev_prix_moyen) / prev_prix_moyen * 100, 1) if prev_prix_moyen else None,
        "ca_couvert_pct": round(ca_produits_total / total_ca_group * 100, 1) if total_ca_group else 0.0,
        "produits_par_marque": produits_par_marque,
        "produits_par_restaurant": produits_par_restaurant,
        "produits_table": produits_agg,
        "top_product": top_product,
        "top_product_qty": top_product_qty,
        "repartition_marques": repartition_marques,
        "restaurants_with_data": list(produits_par_restaurant.keys()),
    }


def _brand_monthly_totals(data):
    """{ brand: { month: total_ca } }"""
    result = {b: {m: 0 for m in MONTHS} for b in BRAND_COLORS}
    for r in RESTAURANTS:
        for m in MONTHS:
            result[r["brand"]][m] += data.get(r["id"], {}).get(m, 0)
    return result


def _restaurant_totals(data):
    """Liste triée par CA total décroissant, avec données mensuelles."""
    rows = []
    for r in RESTAURANTS:
        monthly = {m: data.get(r["id"], {}).get(m, 0) for m in MONTHS}
        rows.append({
            "id":       r["id"],
            "profil_id": r.get("profil_id"),
            "name":     r["name"],
            "brand":    r["brand"],
            "monthly":  monthly,
            "total":    sum(monthly.values()),
        })
    rows.sort(key=lambda x: x["total"], reverse=True)
    for i, row in enumerate(rows):
        row["rank"] = i + 1
    return rows


def _compute_progress(data):
    """
    Calcule le taux d'atteinte objectif pour chaque restaurant/mois.
    Retourne { resto_id: { month: {"obj": X, "ca": Y, "taux": Z} } }
    Seuls les couples (resto, mois) avec un objectif > 0 sont inclus.
    """
    objectives = data.get("objectifs", {})
    result = {}
    for r in RESTAURANTS:
        rid = r["id"]
        obj_by_month = objectives.get(rid, {})
        for m in MONTHS:
            obj = obj_by_month.get(m, 0)
            if obj > 0:
                ca = data.get(rid, {}).get(m, 0)
                taux = round(ca / obj * 100, 1)
                result.setdefault(rid, {})[m] = {"obj": obj, "ca": ca, "taux": taux}
    return result


def _current_month_key():
    """Retourne le mois courant au format YYYY-MM, borné aux mois du dashboard."""
    current_key = datetime.now().strftime("%Y-%m")
    if current_key in MONTHS:
        return current_key

    past_or_equal = [m for m in MONTHS if m <= current_key]
    if past_or_equal:
        return past_or_equal[-1]
    return MONTHS[0]


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    data     = load_data()
    export_mode = request.args.get("export") == "pdf"
    progress = _compute_progress(data)
    brand_monthly = _brand_monthly_totals(data)

    # ── Totaux mensuels groupe ─────────────────────────────────────────────────
    monthly_totals = {
        m: sum(brand_monthly[b][m] for b in brand_monthly)
        for m in MONTHS
    }

    # ── Mois disponibles (au moins 1 CA > 0) ──────────────────────────────────
    mois_disponibles = [m for m in MONTHS if monthly_totals[m] > 0]
    mois_actuel      = _current_month_key()
    mois_actuel_idx  = MONTHS.index(mois_actuel)
    mois_actuel_lbl  = MONTH_LABELS[mois_actuel_idx]
    mois_prec        = MONTHS[mois_actuel_idx - 1] if mois_actuel_idx > 0 else None

    # ── KPIs groupe ───────────────────────────────────────────────────────────
    ca_total_mois      = monthly_totals[mois_actuel]
    ca_total_mois_prec = monthly_totals[mois_prec] if mois_prec else 0
    croissance_groupe  = (
        round((ca_total_mois - ca_total_mois_prec) / ca_total_mois_prec * 100, 1)
        if ca_total_mois_prec else 0
    )
    ca_cumule_annee    = sum(monthly_totals.values())
    nb_restos_actifs = sum(
        1 for r in RESTAURANTS
        if data.get(r["id"], {}).get(mois_actuel, 0) > 0
    )

    # ── Mois pour l'évolution (Jan → mois actuel) ─────────────────────────────
    evol_months = MONTHS[:mois_actuel_idx + 1]

    # ── Données par restaurant ─────────────────────────────────────────────────
    commandes_data  = data.get("commandes", {})
    charges_data    = data.get("charges", {})
    cm_data         = data.get("cout_matieres", {})
    cp_data         = data.get("cout_personnel", {})
    brand_counts = {}
    for r in RESTAURANTS:
        brand_counts[r["brand"]] = brand_counts.get(r["brand"], 0) + 1

    resto_data = []
    for r in RESTAURANTS:
        monthly  = {m: data.get(r["id"], {}).get(m, 0) for m in MONTHS}
        ca_m     = monthly.get(mois_actuel, 0)
        ca_mp    = monthly.get(mois_prec, 0) if mois_prec else 0
        croiss   = round((ca_m - ca_mp) / ca_mp * 100, 1) if ca_mp else None
        prog     = progress.get(r["id"], {}).get(mois_actuel)
        # sparkline 6 derniers mois
        spark    = [monthly[m] for m in evol_months]
        commandes_monthly  = {m: commandes_data.get(r["id"], {}).get(m, 0) or 0 for m in MONTHS}
        charges_monthly    = {m: charges_data.get(r["id"], {}).get(m, 0) or 0 for m in MONTHS}
        cm_monthly         = {m: cm_data.get(r["id"], {}).get(m, 0) or 0 for m in MONTHS}
        cp_monthly         = {m: cp_data.get(r["id"], {}).get(m, 0) or 0 for m in MONTHS}
        # Marge brute % du mois actuel pour ce restaurant
        chg_m = charges_monthly.get(mois_actuel, 0)
        marge_brute_pct = (
            round((ca_m - chg_m) / ca_m * 100, 1)
            if ca_m > 0 and chg_m > 0 else None
        )
        cm_m = cm_monthly.get(mois_actuel, 0)
        cp_m = cp_monthly.get(mois_actuel, 0)
        food_cost_pct  = round(cm_m / ca_m * 100, 1) if ca_m > 0 and cm_m > 0 else None
        labor_cost_pct = round(cp_m / ca_m * 100, 1) if ca_m > 0 and cp_m > 0 else None
        prime_cost_pct = (
            round((cm_m + cp_m) / ca_m * 100, 1)
            if ca_m > 0 and cm_m > 0 and cp_m > 0 else None
        )
        resto_data.append({
            "id":        r["id"],
            "profil_id": r.get("profil_id"),
            "name":      r["name"],
            "brand":     r["brand"],
            "monthly":  monthly,
            "commandes": commandes_monthly,
            "charges":  charges_monthly,
            "cout_matieres":  cm_monthly,
            "cout_personnel": cp_monthly,
            "ca_mois":  ca_m,
            "ca_mois_prec": ca_mp,
            "croissance": croiss,
            "total":    sum(monthly.values()),
            "progress": prog,
            "sparkline": spark,
            "marge_brute_pct": marge_brute_pct,
            "food_cost_pct":   food_cost_pct,
            "labor_cost_pct":  labor_cost_pct,
            "prime_cost_pct":  prime_cost_pct,
        })

    # ── Marge brute groupe ────────────────────────────────────────────────────
    marges = [r["marge_brute_pct"] for r in resto_data if r["marge_brute_pct"] is not None]
    marge_brute_groupe   = round(sum(marges) / len(marges), 1) if marges else None
    nb_restos_marge      = len(marges)

    # ── Food / Labor / Prime Cost groupe ─────────────────────────────────────
    food_costs  = [r["food_cost_pct"]  for r in resto_data if r["food_cost_pct"]  is not None]
    labor_costs = [r["labor_cost_pct"] for r in resto_data if r["labor_cost_pct"] is not None]
    prime_costs = [r["prime_cost_pct"] for r in resto_data if r["prime_cost_pct"] is not None]
    food_cost_groupe   = round(sum(food_costs)  / len(food_costs),  1) if food_costs  else None
    labor_cost_groupe  = round(sum(labor_costs) / len(labor_costs), 1) if labor_costs else None
    prime_cost_groupe  = round(sum(prime_costs) / len(prime_costs), 1) if prime_costs else None
    nb_restos_food_cost  = len(food_costs)
    nb_restos_labor_cost = len(labor_costs)

    # ── KPIs M-1 ──────────────────────────────────────────────────────────────
    mois_prec_lbl = MONTH_LABELS[MONTHS.index(mois_prec)] if mois_prec else None

    # Ticket moyen groupe actuel + M-1
    commandes_actuel_total = sum(
        commandes_data.get(r["id"], {}).get(mois_actuel, 0) or 0 for r in RESTAURANTS
    )
    ticket_moyen_groupe = (
        round(ca_total_mois / commandes_actuel_total, 2)
        if commandes_actuel_total > 0 and ca_total_mois > 0 else None
    )
    if mois_prec:
        commandes_prec_total = sum(
            commandes_data.get(r["id"], {}).get(mois_prec, 0) or 0 for r in RESTAURANTS
        )
        ticket_moyen_groupe_prec = (
            round(ca_total_mois_prec / commandes_prec_total, 2)
            if commandes_prec_total > 0 and ca_total_mois_prec > 0 else None
        )
    else:
        ticket_moyen_groupe_prec = None
    ticket_moyen_evolution_pct = (
        round((ticket_moyen_groupe - ticket_moyen_groupe_prec) / ticket_moyen_groupe_prec * 100, 1)
        if ticket_moyen_groupe is not None and ticket_moyen_groupe_prec and ticket_moyen_groupe_prec > 0
        else None
    )

    # Marge brute groupe M-1
    def _pct_prec(r, field_num, field_den=None):
        """cost_pct = field_num / field_den * 100 pour mois_prec; field_den=None → (ca-chg)/ca."""
        if not mois_prec:
            return None
        ca_p = r["monthly"].get(mois_prec, 0)
        if ca_p <= 0:
            return None
        num = r[field_num].get(mois_prec, 0) if field_num else None
        if field_den:
            den = r[field_den].get(mois_prec, 0)
            if num and den:
                return round(num / ca_p * 100, 1) if field_den == "_ca" else round((num + den) / ca_p * 100, 1)
        else:  # marge brute
            chg = r["charges"].get(mois_prec, 0)
            return round((ca_p - chg) / ca_p * 100, 1) if chg > 0 else None

    mb_prec_vals  = [v for r in resto_data if (v := _pct_prec(r, None)) is not None]
    marge_brute_groupe_prec = round(sum(mb_prec_vals)/len(mb_prec_vals), 1) if mb_prec_vals else None
    marge_brute_evolution_pts = (
        round(marge_brute_groupe - marge_brute_groupe_prec, 1)
        if marge_brute_groupe is not None and marge_brute_groupe_prec is not None else None
    )

    # Food Cost groupe M-1
    fc_prec_vals = []
    for r in resto_data:
        if mois_prec:
            ca_p = r["monthly"].get(mois_prec, 0)
            cm_p = r["cout_matieres"].get(mois_prec, 0)
            if ca_p > 0 and cm_p > 0:
                fc_prec_vals.append(round(cm_p / ca_p * 100, 1))
    food_cost_groupe_prec = round(sum(fc_prec_vals)/len(fc_prec_vals), 1) if fc_prec_vals else None
    food_cost_evolution_pts = (
        round(food_cost_groupe - food_cost_groupe_prec, 1)
        if food_cost_groupe is not None and food_cost_groupe_prec is not None else None
    )

    # Labor Cost groupe M-1
    lc_prec_vals = []
    for r in resto_data:
        if mois_prec:
            ca_p = r["monthly"].get(mois_prec, 0)
            cp_p = r["cout_personnel"].get(mois_prec, 0)
            if ca_p > 0 and cp_p > 0:
                lc_prec_vals.append(round(cp_p / ca_p * 100, 1))
    labor_cost_groupe_prec = round(sum(lc_prec_vals)/len(lc_prec_vals), 1) if lc_prec_vals else None
    labor_cost_evolution_pts = (
        round(labor_cost_groupe - labor_cost_groupe_prec, 1)
        if labor_cost_groupe is not None and labor_cost_groupe_prec is not None else None
    )

    # Prime Cost groupe M-1
    pc_prec_vals = []
    for r in resto_data:
        if mois_prec:
            ca_p = r["monthly"].get(mois_prec, 0)
            cm_p = r["cout_matieres"].get(mois_prec, 0)
            cp_p = r["cout_personnel"].get(mois_prec, 0)
            if ca_p > 0 and cm_p > 0 and cp_p > 0:
                pc_prec_vals.append(round((cm_p + cp_p) / ca_p * 100, 1))
    prime_cost_groupe_prec = round(sum(pc_prec_vals)/len(pc_prec_vals), 1) if pc_prec_vals else None
    prime_cost_evolution_pts = (
        round(prime_cost_groupe - prime_cost_groupe_prec, 1)
        if prime_cost_groupe is not None and prime_cost_groupe_prec is not None else None
    )

    # ── Classements ───────────────────────────────────────────────────────────
    sorted_ca = sorted(resto_data, key=lambda x: x["ca_mois"], reverse=True)
    for i, r in enumerate(sorted_ca):
        r["rank"] = i + 1
    top3  = sorted_ca[:3]
    avec_ca = [r for r in sorted_ca if r["ca_mois"] > 0]
    flop3 = list(reversed((avec_ca[-3:] if len(avec_ca) >= 3 else sorted_ca[-3:])))

    meilleur = sorted_ca[0] if sorted_ca else None
    meilleur_restaurant = {
        "nom":       meilleur["name"],
        "marque":    meilleur["brand"],
        "ca":        meilleur["ca_mois"],
        "croissance": meilleur["croissance"],
    } if meilleur else None

    # ── Par marque ────────────────────────────────────────────────────────────
    grand_total_mois = ca_total_mois or 1
    best_brand_mois  = max(BRAND_COLORS, key=lambda b: brand_monthly[b][mois_actuel])
    marques_data = []
    for brand, color in BRAND_COLORS.items():
        ca_m  = brand_monthly[brand][mois_actuel]
        ca_mp = brand_monthly[brand][mois_prec] if mois_prec else 0
        croiss = round((ca_m - ca_mp) / ca_mp * 100, 1) if ca_mp else 0
        nb    = brand_counts.get(brand, 0)
        marques_data.append({
            "nom":             brand,
            "couleur":         color,
            "ca_mois":         ca_m,
            "ca_mois_prec":    ca_mp,
            "croissance":      croiss,
            "nb_restaurants":  nb,
            "ca_moyen":        round(ca_m / nb) if nb else 0,
            "part_marche":     round(ca_m / grand_total_mois * 100, 1),
            "sparkline":       [brand_monthly[brand][m] for m in evol_months],
            "best":            brand == best_brand_mois,
        })

    # ── Évolution : Jan → mois actuel ────────────────────────────────────────
    evol_lbls   = MONTH_LABELS[:mois_actuel_idx + 1]
    evolution = {
        "mois":    evol_lbls,
        "chamas":  [brand_monthly["Chamas Tacos"][m]  for m in evol_months],
        "ocheese": [brand_monthly["O'Cheese"][m]      for m in evol_months],
        "delys":   [brand_monthly["Delys Station"][m] for m in evol_months],
        "total":   [monthly_totals[m]                 for m in evol_months],
    }

    # ── Classement restaurants (bar chart horizontal) ─────────────────────────
    BAR_COLORS = {
        "Chamas Tacos":  "#E28F0A",
        "O'Cheese":      "#D82E2E",
        "Delys Station": "#DB29C9",
    }
    restaurants_classement = sorted(
        [
            {
                "nom":     r["name"],
                "brand":   r["brand"],
                "ca":      data.get(r["id"], {}).get(mois_actuel, 0) or 0,
                "couleur": BAR_COLORS.get(r["brand"], "#888"),
            }
            for r in RESTAURANTS
            if (data.get(r["id"], {}).get(mois_actuel, 0) or 0) > 0
        ],
        key=lambda x: x["ca"],
        reverse=True,
    )

    # ── Waterfall contribution par marque ─────────────────────────────────────
    if mois_prec:
        wf_prec  = monthly_totals.get(mois_prec, 0)
        wf_actuel = ca_total_mois
        waterfall_data = {
            "ca_groupe_precedent": wf_prec,
            "ca_groupe_actuel":    wf_actuel,
            "contributions": [
                {"marque": b, "delta": brand_monthly[b][mois_actuel] - brand_monthly[b][mois_prec]}
                for b in ["Chamas Tacos", "O'Cheese", "Delys Station"]
            ],
        }
    else:
        waterfall_data = None

    # ── Radar scores par marque ───────────────────────────────────────────────
    def _brand_score(brand):
        restos_brand = [r for r in RESTAURANTS if r["brand"] == brand]
        ca_total_b = sum(data.get(r["id"], {}).get(mois_actuel, 0) or 0 for r in restos_brand)
        if ca_total_b <= 0:
            return None
        nb = len(restos_brand)
        # Ticket moyen
        cmd_b = sum(commandes_data.get(r["id"], {}).get(mois_actuel, 0) or 0 for r in restos_brand)
        tkt_b = ca_total_b / cmd_b if cmd_b > 0 else 0
        # Food / Labor / Prime cost
        cm_b  = sum(cm_data.get(r["id"], {}).get(mois_actuel, 0) or 0 for r in restos_brand)
        cp_b  = sum(cp_data.get(r["id"], {}).get(mois_actuel, 0) or 0 for r in restos_brand)
        fc_b  = round(cm_b / ca_total_b * 100, 1) if cm_b > 0 else None
        lc_b  = round(cp_b / ca_total_b * 100, 1) if cp_b > 0 else None
        pc_b  = round((cm_b + cp_b) / ca_total_b * 100, 1) if (cm_b > 0 and cp_b > 0) else None
        return {
            "ca_raw":    ca_total_b,
            "tkt_raw":   tkt_b,
            "fc_raw":    fc_b,
            "lc_raw":    lc_b,
            "pc_raw":    pc_b,
        }

    raw_scores = {b: _brand_score(b) for b in BRAND_COLORS}
    # Normalisation CA : score basé sur part max
    ca_max = max((v["ca_raw"] for v in raw_scores.values() if v), default=1) or 1
    tkt_max = max((v["tkt_raw"] for v in raw_scores.values() if v), default=1) or 1

    radar_data = {}
    for brand, raw in raw_scores.items():
        if raw is None:
            radar_data[brand] = None
            continue
        radar_data[brand] = {
            "ca_normalise":       round(raw["ca_raw"] / ca_max * 100),
            "ticket_moyen":       round(raw["tkt_raw"] / tkt_max * 100) if tkt_max else 0,
            "food_cost_inverse":  max(0, round(100 - (raw["fc_raw"] or 50) * 2)) if raw["fc_raw"] is not None else None,
            "labor_cost_inverse": max(0, round(100 - (raw["lc_raw"] or 50) * 2)) if raw["lc_raw"] is not None else None,
            "prime_cost_inverse": max(0, round(100 - (raw["pc_raw"] or 60) * 100 / 80)) if raw["pc_raw"] is not None else None,
        }

    # ── Payload JSON pour JS ──────────────────────────────────────────────────
    data_json = {
        "restaurants":     [
            {k: v for k, v in r.items() if k != "sparkline"}
            for r in resto_data
        ],
        "months":          MONTHS,
        "month_labels":    MONTH_LABELS,
        "mois_actuel":     mois_actuel,
        "mois_actuel_lbl": mois_actuel_lbl,
        "mois_prec":       mois_prec,
        "brand_colors":    BRAND_COLORS,
        "monthly_totals":  monthly_totals,
        "mois_disponibles": mois_disponibles,
        "evolution":       evolution,
        "restaurants_classement": restaurants_classement,
        "waterfall_data":  waterfall_data,
        "radar_data":      radar_data,
    }

    html = render_template(
        "dashboard.html",
        export_mode=export_mode,
        # KPIs
        ca_total_mois=ca_total_mois,
        ca_total_mois_prec=ca_total_mois_prec,
        croissance_groupe=croissance_groupe,
        ca_cumule_annee=ca_cumule_annee,
        marge_brute_groupe=marge_brute_groupe,
        nb_restos_marge=nb_restos_marge,
        food_cost_groupe=food_cost_groupe,
        labor_cost_groupe=labor_cost_groupe,
        prime_cost_groupe=prime_cost_groupe,
        nb_restos_food_cost=nb_restos_food_cost,
        nb_restos_labor_cost=nb_restos_labor_cost,
        # M-1
        mois_prec_lbl=mois_prec_lbl,
        ticket_moyen_groupe=ticket_moyen_groupe,
        ticket_moyen_groupe_prec=ticket_moyen_groupe_prec,
        ticket_moyen_evolution_pct=ticket_moyen_evolution_pct,
        marge_brute_groupe_prec=marge_brute_groupe_prec,
        marge_brute_evolution_pts=marge_brute_evolution_pts,
        food_cost_groupe_prec=food_cost_groupe_prec,
        food_cost_evolution_pts=food_cost_evolution_pts,
        labor_cost_groupe_prec=labor_cost_groupe_prec,
        labor_cost_evolution_pts=labor_cost_evolution_pts,
        prime_cost_groupe_prec=prime_cost_groupe_prec,
        prime_cost_evolution_pts=prime_cost_evolution_pts,
        meilleur_restaurant=meilleur_restaurant,
        nb_restos_actifs=nb_restos_actifs,
        nb_restaurants=len(RESTAURANTS),
        # Marques
        marques_data=marques_data,
        brand_colors=BRAND_COLORS,
        # Évolution
        evolution=evolution,
        # Classements
        top3=top3,
        flop3=flop3,
        tous_restos=sorted_ca,
        # Graphiques CEO
        restaurants_classement=restaurants_classement,
        waterfall_data=waterfall_data,
        radar_data=radar_data,
        # Contexte
        mois_actuel=mois_actuel,
        mois_actuel_lbl=mois_actuel_lbl,
        mois_disponibles=mois_disponibles,
        month_labels=MONTH_LABELS,
        months=MONTHS,
        restaurants=RESTAURANTS,
        data_json=data_json,
    )
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"]        = "no-cache"
    resp.headers["Expires"]       = "0"
    return resp


@app.route("/marques")
@login_required
def marques():
    data = load_data()
    brand_monthly = _brand_monthly_totals(data)

    brand_counts   = {}
    for r in RESTAURANTS:
        brand_counts[r["brand"]] = brand_counts.get(r["brand"], 0) + 1

    brand_total_all = {b: sum(brand_monthly[b].values()) for b in brand_monthly}
    grand_total     = sum(brand_total_all.values())
    brand_avg       = {
        b: brand_total_all[b] / (brand_counts[b] * len(MONTHS)) if brand_counts.get(b, 0) else 0
        for b in brand_monthly
    }
    market_share    = {
        b: round(brand_total_all[b] / grand_total * 100, 1) if grand_total else 0
        for b in brand_total_all
    }
    last_month_totals = {b: brand_monthly[b][MONTHS[-1]] for b in brand_monthly}
    best_brand        = max(last_month_totals, key=last_month_totals.get)

    return render_template(
        "marques.html",
        brand_monthly=brand_monthly,
        months=MONTHS,
        month_labels=MONTH_LABELS,
        best_brand=best_brand,
        best_brand_ca=last_month_totals[best_brand],
        brand_colors=BRAND_COLORS,
        brand_avg=brand_avg,
        brand_total_all=brand_total_all,
        market_share=market_share,
        brand_counts=brand_counts,
        last_month_totals=last_month_totals,
        last_month_label=MONTH_LABELS[-1],
    )


@app.route("/restaurants")
@login_required
def restaurants():
    data = load_data()
    restaurant_totals = _restaurant_totals(data)
    progress       = _compute_progress(data)
    charges_data   = data.get("charges", {})
    cm_data        = data.get("cout_matieres", {})
    cp_data        = data.get("cout_personnel", {})

    # Mois actuel = dernier mois avec au moins 1 CA > 0
    monthly_totals_all = {
        m: sum(data.get(r["id"], {}).get(m, 0) for r in RESTAURANTS)
        for m in MONTHS
    }
    mois_dispo  = [m for m in MONTHS if monthly_totals_all[m] > 0]
    last_month  = mois_dispo[-1] if mois_dispo else MONTHS[0]
    last_idx    = MONTHS.index(last_month)

    for r in restaurant_totals:
        r["progress_last"] = progress.get(r["id"], {}).get(last_month)
        ca_lm  = data.get(r["id"], {}).get(last_month, 0) or 0
        chg_lm = charges_data.get(r["id"], {}).get(last_month, 0) or 0
        cm_lm  = cm_data.get(r["id"], {}).get(last_month, 0) or 0
        cp_lm  = cp_data.get(r["id"], {}).get(last_month, 0) or 0
        r["marge_brute_pct"] = (
            round((ca_lm - chg_lm) / ca_lm * 100, 1)
            if ca_lm > 0 and chg_lm > 0 else None
        )
        r["food_cost_pct"]  = round(cm_lm / ca_lm * 100, 1) if ca_lm > 0 and cm_lm > 0 else None
        r["labor_cost_pct"] = round(cp_lm / ca_lm * 100, 1) if ca_lm > 0 and cp_lm > 0 else None
        r["prime_cost_pct"] = (
            round((cm_lm + cp_lm) / ca_lm * 100, 1)
            if ca_lm > 0 and cm_lm > 0 and cp_lm > 0 else None
        )

    return render_template(
        "restaurants.html",
        restaurant_totals=restaurant_totals,
        brand_colors=BRAND_COLORS,
        months=MONTHS,
        month_labels=MONTH_LABELS,
        last_month_label=MONTH_LABELS[last_idx],
    )


@app.route("/produits")
@login_required
def produits():
    data = load_data()
    export_mode = request.args.get("export") == "pdf"
    all_restos = _get_all_restaurants(data)
    resto_map = {r["id"]: r for r in all_restos}
    produits_root = data.get("produits", {})

    product_records = []
    months_with_data = set()
    restaurant_ids_with_data = set()

    for resto_id, by_month in produits_root.items():
        resto = resto_map.get(resto_id)
        if not resto or not isinstance(by_month, dict):
            continue
        for month, items in by_month.items():
            if month not in MONTHS or not isinstance(items, list):
                continue
            for item in items:
                nom = str(item.get("nom", "")).strip()
                quantite = _product_number(item.get("quantite"))
                prix_unitaire = _product_number(item.get("prix_unitaire"))
                if not nom or quantite <= 0 or prix_unitaire < 0:
                    continue
                ca_produit = round(quantite * prix_unitaire, 2)
                product_records.append({
                    "restaurant_id": resto_id,
                    "restaurant_name": resto["name"],
                    "brand": resto["brand"],
                    "month": month,
                    "nom": nom,
                    "quantite": round(quantite, 2),
                    "prix_unitaire": round(prix_unitaire, 2),
                    "ca": ca_produit,
                })
                months_with_data.add(month)
                restaurant_ids_with_data.add(resto_id)

    # ── Données issues de l'import Excel (ventes_produits) ────────────────────
    ventes_produits_root = data.get("ventes_produits", {})
    for month, by_brand in ventes_produits_root.items():
        if month not in MONTHS or not isinstance(by_brand, dict):
            continue
        for brand, brand_data in by_brand.items():
            restaurants_data = brand_data.get("restaurants", {}) if isinstance(brand_data, dict) else {}
            for resto_id, articles in restaurants_data.items():
                resto = resto_map.get(resto_id)
                if not resto or not isinstance(articles, dict):
                    continue
                for article_key, article in articles.items():
                    if not isinstance(article, dict):
                        continue
                    nom = str(article.get("article", "")).strip()
                    ventes = _product_number(article.get("ventes"))
                    quantite_importee = _product_number(article.get("quantite")) if article.get("quantite") is not None else 0.0
                    # Ignorer seulement si pas de CA ET pas de quantité (ex: ligne vide)
                    if not nom or (ventes <= 0 and quantite_importee <= 0):
                        continue
                    if quantite_importee > 0:
                        q = quantite_importee
                        pu = round(ventes / q, 4) if ventes > 0 else 0.0
                    else:
                        # Pas de colonne quantité détectée — on met 0 pour ne pas fausser les totaux
                        q = 0.0
                        pu = 0.0
                    product_records.append({
                        "restaurant_id": resto_id,
                        "restaurant_name": resto["name"],
                        "brand": resto["brand"],
                        "month": month,
                        "nom": nom,
                        "quantite": round(q, 2),
                        "prix_unitaire": pu,
                        "ca": round(ventes, 2),
                    })
                    months_with_data.add(month)
                    restaurant_ids_with_data.add(resto_id)

    mois_disponibles = [m for m in MONTHS if m in months_with_data]
    mois_courant = _current_month_key()
    selected_month = request.args.get("mois", "")
    if selected_month not in mois_disponibles:
        selected_month = mois_courant if mois_courant in mois_disponibles else (mois_disponibles[-1] if mois_disponibles else None)

    prev_month = None
    if selected_month and MONTHS.index(selected_month) > 0:
        prev_month = MONTHS[MONTHS.index(selected_month) - 1]

    total_ca_by_month = {
        month: sum((data.get(r["id"], {}).get(month, 0) or 0) for r in all_restos)
        for month in MONTHS
    }

    current_records = _month_product_records(product_records, selected_month) if selected_month else []
    prev_records = _month_product_records(product_records, prev_month) if prev_month else []
    initial_summary = _build_products_summary(
        current_records,
        prev_records,
        total_ca_by_month.get(selected_month, 0) if selected_month else 0,
        [resto_map[rid] for rid in restaurant_ids_with_data if rid in resto_map],
    ) if selected_month else {
        "top5_produits_groupe": [],
        "top5_produits_volume": [],
        "flop5_produits": [],
        "produit_star_par_marque": {},
        "nb_produits_actifs": 0,
        "ca_produits_total": 0,
        "volume_total_vendu": 0,
        "volume_total_evolution_pct": None,
        "prix_moyen_article": 0,
        "prix_moyen_evolution_pct": None,
        "ca_couvert_pct": 0,
        "produits_par_marque": {brand: [] for brand in PRODUCT_BRAND_COLORS},
        "produits_par_restaurant": {},
        "produits_table": [],
        "top_product": None,
        "top_product_qty": 0,
        "repartition_marques": [],
        "restaurants_with_data": [],
    }

    restaurants_disponibles = [
        {
            "id": resto["id"],
            "name": resto["name"],
            "brand": resto["brand"],
        }
        for resto in all_restos
        if resto["id"] in restaurant_ids_with_data
    ]

    data_json = {
        "current_month": selected_month,
        "current_month_label": MONTH_LABELS[MONTHS.index(selected_month)] if selected_month else None,
        "months_available": mois_disponibles,
        "month_labels": {month: MONTH_LABELS[MONTHS.index(month)] for month in mois_disponibles},
        "brands": list(PRODUCT_BRAND_COLORS.keys()),
        "brand_colors": PRODUCT_BRAND_COLORS,
        "restaurants": restaurants_disponibles,
        "records": product_records,
        "total_ca_by_month": {month: total_ca_by_month.get(month, 0) for month in mois_disponibles},
        "restaurant_ca_by_month": {
            resto["id"]: {month: data.get(resto["id"], {}).get(month, 0) or 0 for month in MONTHS}
            for resto in all_restos
        },
    }

    return render_template(
        "produits.html",
        export_mode=export_mode,
        data_json=data_json,
        marques_disponibles=list(PRODUCT_BRAND_COLORS.keys()),
        mois_disponibles=mois_disponibles,
        restaurants_disponibles=restaurants_disponibles,
        selected_month=selected_month,
        selected_month_label=MONTH_LABELS[MONTHS.index(selected_month)] if selected_month else None,
        product_brand_colors=PRODUCT_BRAND_COLORS,
        total_ca_groupe=total_ca_by_month.get(selected_month, 0) if selected_month else 0,
        **initial_summary,
    )


def _log_historique(data, resto_id, month, nouvelle_valeur, type_action):
    """Ajoute une entrée dans l'historique du JSON."""
    all_r = _get_all_restaurants(data)
    resto = next((r for r in all_r if r["id"] == resto_id), None)
    if not resto:
        return
    ancienne  = data.get(resto_id, {}).get(month, None)
    mois_lbl  = MONTH_LABELS[MONTHS.index(month)] if month in MONTHS else month
    entry = {
        "id":               str(uuid.uuid4())[:8],
        "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "restaurant_id":    resto_id,
        "restaurant_nom":   resto["name"],
        "marque":           resto["brand"],
        "mois":             mois_lbl,
        "mois_key":         month,
        "ancienne_valeur":  ancienne if ancienne else 0,
        "nouvelle_valeur":  nouvelle_valeur,
        "type":             type_action,
    }
    data.setdefault("historique", []).insert(0, entry)


@app.route("/saisie", methods=["GET", "POST"])
@login_required
def saisie():
    """Redirige vers /donnees pour compatibilité ascendante."""
    if request.method == "POST":
        return redirect(url_for("donnees"), code=307)
    args = request.args.to_dict()
    return redirect(url_for("donnees", **args))


@app.route("/donnees", methods=["GET", "POST"])
@login_required
def donnees():
    if request.method == "POST":
        resto_id   = request.form.get("restaurant_id", "").strip()
        month      = request.form.get("month", "").strip()
        ca_raw     = request.form.get("ca", "").strip()
        cmd_raw    = request.form.get("commandes", "").strip()
        chg_raw    = request.form.get("charges", "").strip()
        cf_raw     = request.form.get("couts_fixes", "").strip()
        cv_raw     = request.form.get("couts_variables", "").strip()
        cm_raw     = request.form.get("cout_matieres", "").strip()
        cp_raw     = request.form.get("cout_personnel", "").strip()

        if not resto_id or not month or not ca_raw:
            flash("Tous les champs sont obligatoires.", "error")
            return redirect(url_for("donnees"))

        if month not in ALL_MONTHS:
            flash("Mois invalide.", "error")
            return redirect(url_for("donnees"))

        valid_ids = {r["id"] for r in RESTAURANTS}
        if resto_id not in valid_ids:
            flash("Restaurant invalide.", "error")
            return redirect(url_for("donnees"))

        try:
            ca_value = int(float(ca_raw))
            if ca_value < 0:
                raise ValueError
        except ValueError:
            flash("Le CA doit être un nombre positif.", "error")
            return redirect(url_for("donnees"))

        try:
            cmd_value = int(float(cmd_raw)) if cmd_raw else 0
            if cmd_value < 0:
                raise ValueError
        except ValueError:
            flash("Le nombre de commandes doit être un entier positif.", "error")
            return redirect(url_for("donnees"))

        try:
            chg_value = int(float(chg_raw)) if chg_raw else 0
            if chg_value < 0:
                raise ValueError
        except ValueError:
            flash("Les charges doivent être un nombre positif.", "error")
            return redirect(url_for("donnees"))

        try:
            cf_value = int(float(cf_raw)) if cf_raw else 0
            if cf_value < 0:
                raise ValueError
        except ValueError:
            flash("Les coûts fixes doivent être un nombre positif.", "error")
            return redirect(url_for("donnees"))

        try:
            cv_value = int(float(cv_raw)) if cv_raw else 0
            if cv_value < 0:
                raise ValueError
        except ValueError:
            flash("Les coûts variables doivent être un nombre positif.", "error")
            return redirect(url_for("donnees"))

        try:
            cm_value = int(float(cm_raw)) if cm_raw else 0
            if cm_value < 0:
                raise ValueError
        except ValueError:
            flash("Le coût matières doit être un nombre positif.", "error")
            return redirect(url_for("donnees"))

        try:
            cp_value = int(float(cp_raw)) if cp_raw else 0
            if cp_value < 0:
                raise ValueError
        except ValueError:
            flash("Le coût personnel doit être un nombre positif.", "error")
            return redirect(url_for("donnees"))

        data        = load_data()
        existant    = data.get(resto_id, {}).get(month, 0)
        type_action = "modification" if existant and existant > 0 else "création"
        _log_historique(data, resto_id, month, ca_value, type_action)
        data.setdefault(resto_id, {})[month] = ca_value
        if cmd_value > 0:
            data.setdefault("commandes", {}).setdefault(resto_id, {})[month] = cmd_value
        if chg_value > 0:
            data.setdefault("charges", {}).setdefault(resto_id, {})[month] = chg_value
        if cf_value > 0:
            old_cf = data.get("couts_fixes", {}).get(resto_id, {}).get(month, 0) or 0
            data.setdefault("couts_fixes", {}).setdefault(resto_id, {})[month] = cf_value
            # Propagation aux mois suivants sans valeur
            if cf_value != old_cf:
                mois_idx = ALL_MONTHS.index(month)
                for future_m in ALL_MONTHS[mois_idx + 1:]:
                    existing_cf = data.get("couts_fixes", {}).get(resto_id, {}).get(future_m, 0) or 0
                    if existing_cf == 0:
                        data.setdefault("couts_fixes", {}).setdefault(resto_id, {})[future_m] = cf_value
        if cv_value > 0:
            data.setdefault("couts_variables", {}).setdefault(resto_id, {})[month] = cv_value
        if cm_value > 0:
            data.setdefault("cout_matieres", {}).setdefault(resto_id, {})[month] = cm_value
        if cp_value > 0:
            data.setdefault("cout_personnel", {}).setdefault(resto_id, {})[month] = cp_value
        _save_data(data)

        resto_name  = next(r["name"] for r in RESTAURANTS if r["id"] == resto_id)
        month_label = ALL_MONTH_LABELS[ALL_MONTHS.index(month)]
        parts = []
        if cmd_value > 0:
            tm = round(ca_value / cmd_value, 2)
            parts.append(f"Ticket moyen\u00a0: {tm:,.2f}\u00a0€".replace(",", "\u202f"))
        if chg_value > 0 and ca_value > 0:
            mb = round((ca_value - chg_value) / ca_value * 100, 1)
            parts.append(f"Marge brute\u00a0: {mb}\u00a0%")
        if cm_value > 0 and ca_value > 0:
            fc = round(cm_value / ca_value * 100, 1)
            parts.append(f"Food Cost\u00a0: {fc}\u00a0%")
        if cp_value > 0 and ca_value > 0:
            lc = round(cp_value / ca_value * 100, 1)
            parts.append(f"Labor Cost\u00a0: {lc}\u00a0%")
        if cm_value > 0 and cp_value > 0 and ca_value > 0:
            pc = round((cm_value + cp_value) / ca_value * 100, 1)
            parts.append(f"Prime Cost\u00a0: {pc}\u00a0%")
        if parts:
            msg = f"Données enregistrées — {' · '.join(parts)} ({resto_name}, {month_label})."
        else:
            msg = f"CA de {ca_value:,}\u00a0€ enregistré pour {resto_name} ({month_label}).".replace(",", "\u202f")
        flash(msg, "success")
        return redirect(url_for("donnees"))

    data        = load_data()
    all_restos  = _get_all_restaurants(data)
    today_m     = datetime.now().strftime("%Y-%m")
    # Tous les mois jusqu'à aujourd'hui (2025 + 2026 partiel)
    today_idx    = ALL_MONTHS.index(today_m) if today_m in ALL_MONTHS else len(ALL_MONTHS) - 1
    months_avail = ALL_MONTHS[:today_idx + 1]
    labels_avail = ALL_MONTH_LABELS[:today_idx + 1]
    # Séparation par année pour les onglets du sélecteur
    months_2025_avail = [(m, l) for m, l in zip(months_avail, labels_avail) if m.startswith("2025")]
    months_2026_avail = [(m, l) for m, l in zip(months_avail, labels_avail) if m.startswith("2026")]

    selected_import_brand = request.args.get("import_brand", list(BRAND_COLORS.keys())[0])
    if selected_import_brand not in BRAND_COLORS:
        selected_import_brand = list(BRAND_COLORS.keys())[0]

    selected = request.args.get("mois", "")
    if selected not in months_avail:
        mo_totals = {
            m: sum((data.get(r["id"], {}).get(m, 0) or 0) for r in all_restos)
            for m in months_avail
        }
        dispo    = [m for m in months_avail if mo_totals[m] > 0]
        selected = dispo[-1] if dispo else months_avail[-1]

    sel_idx = ALL_MONTHS.index(selected)

    all_data = {}
    for r in all_restos:
        all_data[r["id"]] = {
            "ca":              {m: data.get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
            "commandes":       {m: data.get("commandes", {}).get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
            "charges":         {m: data.get("charges", {}).get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
            "couts_fixes":     {m: data.get("couts_fixes", {}).get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
            "couts_variables": {m: data.get("couts_variables", {}).get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
            "cout_matieres":   {m: data.get("cout_matieres", {}).get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
            "cout_personnel":  {m: data.get("cout_personnel", {}).get(r["id"], {}).get(m, 0) or 0 for m in ALL_MONTHS},
        }

    return render_template(
        "donnees.html",
        restaurants=all_restos,
        import_brands=list(BRAND_COLORS.keys()),
        selected_import_brand=selected_import_brand,
        product_sales_summary=_product_sales_summary(data, selected, selected_import_brand),
        months=ALL_MONTHS,
        month_labels=ALL_MONTH_LABELS,
        months_with_labels=list(zip(months_avail, labels_avail)),
        months_2025=months_2025_avail,
        months_2026=months_2026_avail,
        selected_month=selected,
        selected_month_label=ALL_MONTH_LABELS[sel_idx],
        all_data=all_data,
        brand_colors=BRAND_COLORS,
    )


@app.route("/donnees/import-produits", methods=["POST"])
@login_required
def donnees_import_produits():
    data = load_data()
    all_restos = _get_all_restaurants(data)

    brand = request.form.get("brand", "").strip()
    month = request.form.get("month", "").strip()
    file = request.files.get("excel_file")

    if brand not in BRAND_COLORS:
        flash("Sélectionnez une marque valide pour l'import produits.", "error")
        return redirect(url_for("donnees", mois=month or _current_month_key()))

    if month not in MONTHS:
        flash("Sélectionnez un mois valide pour l'import produits.", "error")
        return redirect(url_for("donnees", mois=_current_month_key(), import_brand=brand))

    if file is None or not file.filename:
        flash("Ajoutez un fichier Excel `.xlsx` pour importer les ventes produits.", "error")
        return redirect(url_for("donnees", mois=month, import_brand=brand))

    if not file.filename.lower().endswith(".xlsx"):
        flash("Seuls les fichiers Excel `.xlsx` sont pris en charge pour le moment.", "error")
        return redirect(url_for("donnees", mois=month, import_brand=brand))

    brand_restos = [r for r in all_restos if r.get("brand") == brand]
    try:
        imported = _import_product_sales_xlsx(file, brand, month, brand_restos)
    except (ValueError, zipfile.BadZipFile) as exc:
        flash(f"Import impossible : {exc}", "error")
        return redirect(url_for("donnees", mois=month, import_brand=brand))

    data.setdefault("ventes_produits", {}).setdefault(month, {})[brand] = {
        "restaurants": imported["restaurants"],
    }
    data.setdefault("ventes_produits_imports", {}).setdefault(month, {})[brand] = {
        "filename": file.filename,
        "imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stats": imported["stats"],
    }
    _save_data(data)

    stats = imported["stats"]
    flash(
        (
            f"Import ventes produits terminé pour {brand} ({MONTH_LABELS[MONTHS.index(month)]}) : "
            f"{stats['rows_kept']} lignes positives conservées, "
            f"{stats['rows_negative_skipped']} lignes négatives ignorées, "
            f"{stats['rows_zero_skipped']} lignes à 0 ignorées, "
            f"{stats['rows_unknown_restaurant']} lignes sans restaurant reconnu."
        ),
        "success",
    )
    return redirect(url_for("donnees", mois=month, import_brand=brand))


@app.route("/reset", methods=["POST"])
@login_required
def reset_data():
    data = load_data()
    # Conserve objectifs et historique, remet tous les CA à 0
    objectifs  = data.get("objectifs", {})
    historique = data.get("historique", [])
    new_data   = {r["id"]: {m: 0 for m in MONTHS} for r in RESTAURANTS}
    new_data["objectifs"]  = objectifs
    new_data["historique"] = historique
    # Log dans l'historique
    new_data["historique"].insert(0, {
        "id":             str(uuid.uuid4())[:8],
        "timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "restaurant_id":  "all",
        "restaurant_nom": "Tous les restaurants",
        "marque":         "Toutes",
        "mois":           "Toutes périodes",
        "mois_key":       "",
        "ancienne_valeur": 0,
        "nouvelle_valeur": 0,
        "type":           "réinitialisation",
    })
    _save_data(new_data)
    flash("Tous les chiffres ont été réinitialisés.", "success")
    return redirect(url_for("donnees"))


@app.route("/objectifs", methods=["GET", "POST"])
@login_required
def objectifs():
    if request.method == "POST":
        resto_id = request.form.get("restaurant_id", "").strip()
        month    = request.form.get("month", "").strip()
        obj_raw  = request.form.get("objectif", "").strip()

        if not resto_id or not month or not obj_raw:
            flash("Tous les champs sont obligatoires.", "error")
            return redirect(url_for("objectifs"))

        if month not in MONTHS:
            flash("Mois invalide.", "error")
            return redirect(url_for("objectifs"))

        valid_ids = {r["id"] for r in RESTAURANTS}
        if resto_id not in valid_ids:
            flash("Restaurant invalide.", "error")
            return redirect(url_for("objectifs"))

        try:
            obj_value = int(float(obj_raw))
            if obj_value <= 0:
                raise ValueError
        except ValueError:
            flash("L'objectif doit être un nombre positif.", "error")
            return redirect(url_for("objectifs"))

        data        = load_data()
        existant    = data.get("objectifs", {}).get(resto_id, {}).get(month, 0)
        type_action = "modification" if existant and existant > 0 else "création"
        _log_historique(data, resto_id, month, obj_value, type_action)
        data.setdefault("objectifs", {}).setdefault(resto_id, {})[month] = obj_value
        _save_data(data)

        resto_name  = next(r["name"] for r in RESTAURANTS if r["id"] == resto_id)
        month_label = MONTH_LABELS[MONTHS.index(month)]
        flash(
            f"Objectif de {obj_value:,} € enregistré pour {resto_name} ({month_label}).".replace(",", "\u202f"),
            "success",
        )
        return redirect(url_for("objectifs"))

    data = load_data()
    progress = _compute_progress(data)
    resto_map = {r["id"]: r for r in RESTAURANTS}

    # Tableau récap : tous les objectifs fixés, triés par resto puis mois
    recap = []
    for rid, months_data in progress.items():
        r = resto_map[rid]
        for m, p in sorted(months_data.items()):
            recap.append({
                "resto_id":    rid,
                "name":        r["name"],
                "brand":       r["brand"],
                "month":       m,
                "month_label": MONTH_LABELS[MONTHS.index(m)],
                "obj":         p["obj"],
                "ca":          p["ca"],
                "taux":        p["taux"],
            })

    return render_template(
        "objectifs.html",
        restaurants=RESTAURANTS,
        months=MONTHS,
        month_labels=MONTH_LABELS,
        brand_colors=BRAND_COLORS,
        recap=recap,
    )


@app.route("/historique")
@login_required
def historique():
    data    = load_data()
    entries = data.get("historique", [])

    # Calcul écart pour chaque entrée
    today     = datetime.now().date()
    yesterday = today - timedelta(days=1)
    for e in entries:
        anc = e.get("ancienne_valeur", 0) or 0
        nouv = e.get("nouvelle_valeur", 0) or 0
        e["ecart_euros"] = nouv - anc
        e["ecart_pct"]   = round((nouv - anc) / anc * 100, 1) if anc else None
        # Label de date
        try:
            d = datetime.strptime(e["timestamp"], "%Y-%m-%d %H:%M:%S").date()
            if d == today:
                e["date_label"] = "Aujourd'hui"
            elif d == yesterday:
                e["date_label"] = "Hier"
            else:
                e["date_label"] = d.strftime("%-d %B %Y")
            e["date_key"] = str(d)
        except Exception:
            e["date_label"] = "—"
            e["date_key"]   = "0000-00-00"

    mois_dispo  = sorted({e["mois_key"] for e in entries if "mois_key" in e})
    mois_labels = {m: MONTH_LABELS[MONTHS.index(m)] for m in mois_dispo if m in MONTHS}

    return render_template(
        "historique.html",
        entries=entries,
        brand_colors=BRAND_COLORS,
        mois_labels=mois_labels,
        total=len(entries),
    )


@app.route("/calendrier")
@login_required
def calendrier():
    data = load_data()

    # Mois disponibles (encodés ou futurs jusqu'au dernier de l'année)
    today_m    = datetime.now().strftime("%Y-%m")
    today_idx  = MONTHS.index(today_m) if today_m in MONTHS else len(MONTHS) - 1

    brand_counts = {}
    for r in RESTAURANTS:
        brand_counts[r["brand"]] = brand_counts.get(r["brand"], 0) + 1

    # Grille : {resto_id: {month: ca or None}}
    grid = {}
    for r in RESTAURANTS:
        grid[r["id"]] = {}
        for i, m in enumerate(MONTHS):
            ca = data.get(r["id"], {}).get(m, 0)
            if i > today_idx:
                grid[r["id"]][m] = None   # futur
            else:
                grid[r["id"]][m] = ca

    # Mois actuel = dernier mois avec au moins 1 CA > 0
    mois_actuel = MONTHS[0]
    for m in MONTHS[:today_idx + 1]:
        if any(data.get(r["id"], {}).get(m, 0) > 0 for r in RESTAURANTS):
            mois_actuel = m
    mois_actuel_lbl = MONTH_LABELS[MONTHS.index(mois_actuel)]

    # Taux complétion mois actuel
    nb_encodes    = sum(1 for r in RESTAURANTS if (data.get(r["id"], {}).get(mois_actuel, 0) or 0) > 0)
    nb_total      = len(RESTAURANTS)
    taux_global   = round(nb_encodes / nb_total * 100) if nb_total else 0
    manquants     = [r for r in RESTAURANTS if not (data.get(r["id"], {}).get(mois_actuel, 0) or 0) > 0]

    # Par marque
    brand_completion = {}
    for brand in BRAND_COLORS:
        restos_brand = [r for r in RESTAURANTS if r["brand"] == brand]
        enc = sum(1 for r in restos_brand if (data.get(r["id"], {}).get(mois_actuel, 0) or 0) > 0)
        brand_completion[brand] = {
            "encodes": enc,
            "total":   len(restos_brand),
            "taux":    round(enc / len(restos_brand) * 100) if restos_brand else 0,
        }

    # Stats saisie
    # Mois avec meilleure complétion
    best_mois_taux, best_mois_lbl = 0, "—"
    for i, m in enumerate(MONTHS[:today_idx + 1]):
        enc = sum(1 for r in RESTAURANTS if (data.get(r["id"], {}).get(m, 0) or 0) > 0)
        t   = round(enc / nb_total * 100) if nb_total else 0
        if t >= best_mois_taux:
            best_mois_taux = t
            best_mois_lbl  = MONTH_LABELS[i]

    # Restaurant le plus souvent manquant
    missing_count = {}
    for r in RESTAURANTS:
        missing_count[r["id"]] = sum(
            1 for m in MONTHS[:today_idx + 1]
            if not (data.get(r["id"], {}).get(m, 0) or 0) > 0
        )
    most_missing_id  = max(missing_count, key=missing_count.get) if missing_count else None
    most_missing     = next((r for r in RESTAURANTS if r["id"] == most_missing_id), None)
    most_missing_nb  = missing_count.get(most_missing_id, 0)

    return render_template(
        "calendrier.html",
        restaurants=RESTAURANTS,
        months=MONTHS,
        month_labels=MONTH_LABELS,
        today_idx=today_idx,
        brand_colors=BRAND_COLORS,
        grid=grid,
        mois_actuel=mois_actuel,
        mois_actuel_lbl=mois_actuel_lbl,
        nb_encodes=nb_encodes,
        nb_total=nb_total,
        taux_global=taux_global,
        manquants=manquants,
        brand_completion=brand_completion,
        best_mois_lbl=best_mois_lbl,
        best_mois_taux=best_mois_taux,
        most_missing=most_missing,
        most_missing_nb=most_missing_nb,
    )


@app.route("/donnees/bulk", methods=["POST"])
@login_required
def donnees_bulk():
    """Sauvegarde en masse depuis la grille interactive."""
    payload    = request.get_json(force=True, silent=True) or {}
    mois       = payload.get("mois", "")
    grid_data  = payload.get("data", {})

    if mois not in ALL_MONTHS:
        return jsonify({"success": False, "error": "Mois invalide"}), 400

    data       = load_data()
    all_restos = _get_all_restaurants(data)
    valid_ids  = {r["id"] for r in all_restos}
    nb_updated = 0

    for resto_id, vals in grid_data.items():
        if resto_id not in valid_ids:
            continue

        ca  = int(vals.get("ca")  or 0)
        cmd = int(vals.get("commandes")     or 0)
        chg = int(vals.get("charges")       or 0)
        cf  = int(vals.get("couts_fixes")   or 0)
        cv  = int(vals.get("couts_variables") or 0)
        cm  = int(vals.get("cout_matieres") or 0)
        cp  = int(vals.get("cout_personnel") or 0)

        old_ca  = data.get(resto_id, {}).get(mois, 0) or 0
        old_cmd = data.get("commandes",      {}).get(resto_id, {}).get(mois, 0) or 0
        old_chg = data.get("charges",        {}).get(resto_id, {}).get(mois, 0) or 0
        old_cf  = data.get("couts_fixes",    {}).get(resto_id, {}).get(mois, 0) or 0
        old_cv  = data.get("couts_variables",{}).get(resto_id, {}).get(mois, 0) or 0
        old_cm  = data.get("cout_matieres",  {}).get(resto_id, {}).get(mois, 0) or 0
        old_cp  = data.get("cout_personnel", {}).get(resto_id, {}).get(mois, 0) or 0

        changed = any([ca != old_ca, cmd != old_cmd, chg != old_chg,
                       cf != old_cf, cv != old_cv, cm != old_cm, cp != old_cp])
        if not changed:
            continue

        nb_updated += 1

        # CA — toujours enregistrer; logger si changement
        if ca != old_ca:
            type_action = "modification" if old_ca > 0 else "création"
            _log_historique(data, resto_id, mois, ca, type_action)
        data.setdefault(resto_id, {})[mois] = ca

        # Champs optionnels : enregistrer si > 0, supprimer si remis à 0
        for key, val in [("commandes", cmd), ("charges", chg),
                         ("couts_fixes", cf), ("couts_variables", cv),
                         ("cout_matieres", cm), ("cout_personnel", cp)]:
            if val > 0:
                data.setdefault(key, {}).setdefault(resto_id, {})[mois] = val
            else:
                data.get(key, {}).get(resto_id, {}).pop(mois, None)

        # Propagation automatique des coûts fixes aux mois suivants
        # (uniquement si le mois suivant n'a pas encore de valeur saisie)
        if cf > 0 and cf != old_cf:
            mois_idx = ALL_MONTHS.index(mois)
            for future_m in ALL_MONTHS[mois_idx + 1:]:
                existing_cf = data.get("couts_fixes", {}).get(resto_id, {}).get(future_m, 0) or 0
                if existing_cf == 0:
                    data.setdefault("couts_fixes", {}).setdefault(resto_id, {})[future_m] = cf

    _save_data(data)

    # ── Alertes post-saisie : évolutions pour les restos mis à jour ───────────
    mois_idx  = MONTHS.index(mois)
    mois_prec = MONTHS[mois_idx - 1] if mois_idx > 0 else None
    post_alertes = []

    if mois_prec and nb_updated > 0:
        data_saved = load_data()

        def _fv(v):
            return str(abs(round(v, 1))).replace(".", ",")

        def _fe(v):
            """Format euro amount: €12 345"""
            return "€" + f"{int(round(v)):,}".replace(",", "\u202f")

        def _fp(pct):
            """Format percentage: 27,3%"""
            return str(round(pct, 1)).replace(".", ",") + "%"

        updated_ids = {
            rid for rid, vals in grid_data.items()
            if rid in valid_ids and any(v for v in vals.values())
        }
        for r in all_restos:
            if r["id"] not in updated_ids:
                continue
            rid   = r["id"]
            name  = r["name"]
            ca_m  = data_saved.get(rid, {}).get(mois, 0) or 0
            ca_mp = data_saved.get(rid, {}).get(mois_prec, 0) or 0
            if ca_m <= 0 or ca_mp <= 0:
                continue

            # CA evolution
            evol_ca = round((ca_m - ca_mp) / ca_mp * 100, 1)
            if evol_ca > 10:
                post_alertes.append({"type": "success",
                    "message": f"Le CA de {name} a progressé de +{_fv(evol_ca)}% (de {_fe(ca_mp)} à {_fe(ca_m)})"})
            elif evol_ca < -10:
                post_alertes.append({"type": "danger",
                    "message": f"Le CA de {name} a diminué de -{_fv(evol_ca)}% (de {_fe(ca_mp)} à {_fe(ca_m)})"})

            # Food Cost evolution
            cm_m  = data_saved.get("cout_matieres", {}).get(rid, {}).get(mois, 0) or 0
            cm_mp = data_saved.get("cout_matieres", {}).get(rid, {}).get(mois_prec, 0) or 0
            if cm_m > 0 and cm_mp > 0 and ca_m > 0 and ca_mp > 0:
                fc_mp   = round(cm_mp / ca_mp * 100, 1)
                fc_m    = round(cm_m  / ca_m  * 100, 1)
                evol_fc = round(fc_m - fc_mp, 1)
                if evol_fc > 3:
                    post_alertes.append({"type": "warning",
                        "message": f"Le Food Cost de {name} a augmenté de +{_fv(evol_fc)}% (de {_fp(fc_mp)} à {_fp(fc_m)})"})
                elif evol_fc < -3:
                    post_alertes.append({"type": "success",
                        "message": f"Le Food Cost de {name} a diminué de -{_fv(evol_fc)}% (de {_fp(fc_mp)} à {_fp(fc_m)})"})

            # Labor Cost evolution
            cp_m  = data_saved.get("cout_personnel", {}).get(rid, {}).get(mois, 0) or 0
            cp_mp = data_saved.get("cout_personnel", {}).get(rid, {}).get(mois_prec, 0) or 0
            if cp_m > 0 and cp_mp > 0 and ca_m > 0 and ca_mp > 0:
                lc_mp   = round(cp_mp / ca_mp * 100, 1)
                lc_m    = round(cp_m  / ca_m  * 100, 1)
                evol_lc = round(lc_m - lc_mp, 1)
                if evol_lc > 3:
                    post_alertes.append({"type": "warning",
                        "message": f"Le Labor Cost de {name} a augmenté de +{_fv(evol_lc)}% (de {_fp(lc_mp)} à {_fp(lc_m)})"})
                elif evol_lc < -3:
                    post_alertes.append({"type": "success",
                        "message": f"Le Labor Cost de {name} a diminué de -{_fv(evol_lc)}% (de {_fp(lc_mp)} à {_fp(lc_m)})"})

        post_alertes = post_alertes[:8]

    return jsonify({"success": True, "nb_updated": nb_updated, "alertes": post_alertes})


@app.route("/analyser/<mois>")
@login_required
def analyser_mois(mois):
    """Retourne un résumé JSON du mois : positifs, négatifs, à surveiller."""
    if mois not in MONTHS:
        return jsonify({"error": "Mois invalide"}), 400

    data          = load_data()
    mois_idx      = MONTHS.index(mois)
    mois_prec     = MONTHS[mois_idx - 1] if mois_idx > 0 else None
    mois_lbl      = MONTH_LABELS[mois_idx]
    all_restos    = _get_all_restaurants(data)
    commandes_d   = data.get("commandes", {})
    charges_d     = data.get("charges", {})
    cm_d          = data.get("cout_matieres", {})
    cp_d          = data.get("cout_personnel", {})

    positifs   = []
    negatifs   = []
    surveiller = []

    for r in all_restos:
        rid  = r["id"]
        name = r["name"]
        ca_m = data.get(rid, {}).get(mois, 0) or 0
        if ca_m <= 0:
            continue

        # CA evolution vs M-1
        if mois_prec:
            ca_mp = data.get(rid, {}).get(mois_prec, 0) or 0
            if ca_mp > 0:
                evol = round((ca_m - ca_mp) / ca_mp * 100, 1)
                evol_str = str(abs(evol)).replace(".", ",")
                if evol > 10:
                    positifs.append(f"CA de {name} en hausse de +{evol_str}%")
                elif evol < -10:
                    negatifs.append(f"CA de {name} en baisse de -{evol_str}%")
                if evol < -10:
                    surveiller.append(f"{name} — CA -{evol_str}% vs mois précédent")

        # Prime Cost
        cm_m = cm_d.get(rid, {}).get(mois, 0) or 0
        cp_m = cp_d.get(rid, {}).get(mois, 0) or 0
        if cm_m > 0 and cp_m > 0 and ca_m > 0:
            pc = round((cm_m + cp_m) / ca_m * 100, 1)
            pc_str = str(pc).replace(".", ",")
            if pc < 50:
                positifs.append(f"Prime Cost de {name} excellent : {pc_str}%")
            elif pc > 60:
                negatifs.append(f"Prime Cost de {name} élevé : {pc_str}%")
                surveiller.append(f"{name} — Prime Cost {pc_str}%")

        # Food Cost
        if cm_m > 0 and ca_m > 0:
            fc = round(cm_m / ca_m * 100, 1)
            fc_str = str(fc).replace(".", ",")
            if fc > 35:
                negatifs.append(f"Food Cost de {name} : {fc_str}% (seuil 35%)")

        # Labor Cost
        if cp_m > 0 and ca_m > 0:
            lc = round(cp_m / ca_m * 100, 1)
            lc_str = str(lc).replace(".", ",")
            if lc > 35:
                negatifs.append(f"Labor Cost de {name} : {lc_str}% (seuil 35%)")

    return jsonify({
        "mois": mois_lbl,
        "positifs":   positifs[:10],
        "negatifs":   negatifs[:10],
        "surveiller": surveiller[:8],
    })


@app.route("/donnees/restaurant/add", methods=["POST"])
@login_required
def add_restaurant():
    """Ajoute un nouveau restaurant dans ca_data.json (custom_restaurants)."""
    name  = request.form.get("name",  "").strip()
    brand = request.form.get("brand", "").strip()
    mois  = request.form.get("mois",  "")

    if not name or brand not in BRAND_COLORS:
        flash("Nom et marque requis.", "error")
        return redirect(url_for("donnees", mois=mois))

    data   = load_data()
    custom = data.setdefault("custom_restaurants", [])

    if any(r.get("name") == name for r in custom + list(RESTAURANTS)):
        flash(f"Un restaurant nommé « {name} » existe déjà.", "error")
        return redirect(url_for("donnees", mois=mois))

    new_id = "cr_" + str(uuid.uuid4())[:8]
    custom.append({"id": new_id, "name": name, "brand": brand})
    data.setdefault(new_id, {m: 0 for m in MONTHS})
    _save_data(data)

    flash(f"Restaurant « {name} » ajouté avec succès.", "success")
    return redirect(url_for("donnees", mois=mois))


@app.route("/restaurant/<int:profil_id>")
@login_required
def restaurant_profil(profil_id):
    resto = next((r for r in RESTAURANTS if r.get("profil_id") == profil_id), None)
    if not resto:
        return "Restaurant introuvable", 404

    data        = load_data()
    export_mode = request.args.get("export") == "pdf"
    selected_month = request.args.get("mois")  # e.g. "2026-04" or None
    rid         = resto["id"]
    brand       = resto["brand"]
    color       = BRAND_COLORS.get(brand, "#888")

    today_m  = datetime.now().strftime("%Y-%m")

    cm_data  = data.get("cout_matieres",   {}).get(rid, {})
    cp_data  = data.get("cout_personnel",  {}).get(rid, {})
    chg_data = data.get("charges",         {}).get(rid, {})
    cf_data  = data.get("couts_fixes",     {}).get(rid, {})
    cv_data  = data.get("couts_variables", {}).get(rid, {})
    cmd_data = data.get("commandes",       {}).get(rid, {})
    ca_data  = data.get(rid, {})

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _safe_pct(num, den):
        return round(num / den * 100, 1) if den and den > 0 and num and num > 0 else None

    def _evol_pct(v, vp):
        return round((v - vp) / vp * 100, 1) if vp and vp > 0 and v is not None else None

    def _evol_pts(v, vp):
        return round(v - vp, 1) if v is not None and vp is not None else None

    def _evol_abs(v, vp):
        """Variation absolue arrondie à l'entier."""
        return round(v - vp) if v is not None and vp is not None else None

    # ── Helper : données d'un mois ────────────────────────────────────────────
    def _month_kpis(m):
        """Retourne les KPIs bruts pour un mois donné (ou None si CA=0)."""
        ca = ca_data.get(m) or 0
        if ca <= 0:
            return None
        cmd = cmd_data.get(m) or 0
        cm  = cm_data.get(m)  or 0
        cp  = cp_data.get(m)  or 0
        cf  = cf_data.get(m)  or 0
        cv  = cv_data.get(m)  or 0
        chg_legacy = chg_data.get(m) or 0
        chg = (cf + cv) if (cf > 0 or cv > 0) else chg_legacy
        tkt = round(ca / cmd, 2)      if cmd > 0 else None
        mb  = _safe_pct(ca - chg, ca) if chg > 0 else None
        fc  = _safe_pct(cm, ca)
        lc  = _safe_pct(cp, ca)
        pc  = round((cm + cp) / ca * 100, 1) if ca > 0 and (cm > 0 or cp > 0) else None
        mn     = ca - cm - cp - chg
        mn_pct = round(mn / ca * 100, 1) if ca > 0 else None
        return dict(ca=ca, cmd=cmd, cm=cm, cp=cp, cf=cf, cv=cv, chg=chg,
                    tkt=tkt, mb=mb, fc=fc, lc=lc, pc=pc, mn=mn, mn_pct=mn_pct)

    # ── Données mensuelles (tous les mois 2025+2026 avec CA > 0) ─────────────
    mois_data = []
    for i, m in enumerate(ALL_MONTHS):
        if m > today_m:          # bloquer les mois futurs
            continue
        k = _month_kpis(m)
        if k is None:
            continue

        ca, cmd, cm, cp, cf, cv, chg = k["ca"], k["cmd"], k["cm"], k["cp"], k["cf"], k["cv"], k["chg"]
        tkt, mb, fc, lc, pc, mn, mn_pct = k["tkt"], k["mb"], k["fc"], k["lc"], k["pc"], k["mn"], k["mn_pct"]

        # ── M-1 (mois précédent calendaire) ──────────────────────────────────
        prev_m = ALL_MONTHS[i - 1] if i > 0 else None
        p  = _month_kpis(prev_m) if prev_m else None
        ca_p    = p["ca"]  if p else 0
        tkt_p   = p["tkt"] if p else None
        mb_p    = p["mb"]  if p else None
        fc_p    = p["fc"]  if p else None
        lc_p    = p["lc"]  if p else None
        pc_p    = p["pc"]  if p else None
        mn_p    = p["mn"]  if p else None
        mn_pct_p = p["mn_pct"] if p else None
        cm_p    = p["cm"]  if p else 0
        cp_p    = p["cp"]  if p else 0
        cf_p    = p["cf"]  if p else 0
        cv_p    = p["cv"]  if p else 0
        chg_p   = p["chg"] if p else 0
        has_prev = ca_p > 0

        # ── N-1 (même mois l'année précédente) ───────────────────────────────
        year   = int(m[:4])
        n1_m   = f"{year - 1}{m[4:]}"  # e.g. "2026-04" → "2025-04"
        n1     = _month_kpis(n1_m) if n1_m in ALL_MONTHS else None
        ca_n1       = n1["ca"]     if n1 else None
        mn_n1       = n1["mn"]     if n1 else None
        mn_pct_n1   = n1["mn_pct"] if n1 else None
        fc_n1       = n1["fc"]     if n1 else None
        lc_n1       = n1["lc"]     if n1 else None
        tkt_n1      = n1["tkt"]    if n1 else None
        cf_n1       = n1["cf"]     if n1 else None
        cv_n1       = n1["cv"]     if n1 else None

        mois_data.append({
            "mois":         m,
            "label":        ALL_MONTH_LABELS[i],
            "annee":        str(year),
            "ca":           ca,
            "cmd":          cmd,
            "cm":           cm,
            "cp":           cp,
            "cf":           cf,
            "cv":           cv,
            "chg":          chg,
            "tkt":          tkt,
            "mb":           mb,
            "fc":           fc,
            "lc":           lc,
            "pc":           pc,
            "mn":           mn,
            "mn_pct":       mn_pct,
            # M-1 (mois précédent)
            "evol_ca":      _evol_pct(ca,  ca_p)      if has_prev else None,
            "evol_ca_abs":  _evol_abs(ca,  ca_p)      if has_prev else None,
            "evol_tkt":     _evol_pct(tkt, tkt_p)     if (tkt and tkt_p) else None,
            "evol_tkt_abs": _evol_abs(tkt, tkt_p)     if (tkt and tkt_p) else None,
            "evol_mb":      _evol_pts(mb,  mb_p),
            "evol_fc":      _evol_pts(fc,  fc_p),
            "evol_lc":      _evol_pts(lc,  lc_p),
            "evol_pc":      _evol_pts(pc,  pc_p),
            "evol_mn_pct":  _evol_pts(mn_pct, mn_pct_p),
            "evol_cm_abs":  _evol_abs(cm,  cm_p)      if has_prev else None,
            "evol_cp_abs":  _evol_abs(cp,  cp_p)      if has_prev else None,
            "evol_chg_abs": _evol_abs(chg, chg_p)     if has_prev else None,
            "evol_mn_abs":  _evol_abs(mn,  mn_p)      if (mn_p is not None) else None,
            "evol_mn_rel":  _evol_pct(mn,  mn_p)      if (mn_p and mn_p > 0) else None,
            # N-1 (même mois année précédente)
            "n1_ca":        ca_n1,
            "n1_ca_abs":    _evol_abs(ca,  ca_n1)     if ca_n1 else None,
            "n1_ca_pct":    _evol_pct(ca,  ca_n1)     if ca_n1 else None,
            "n1_mn":        mn_n1,
            "n1_mn_abs":    _evol_abs(mn,  mn_n1)     if mn_n1 is not None else None,
            "n1_mn_pct":    _evol_pct(mn,  mn_n1)     if (mn_n1 is not None and mn_n1 != 0) else None,
            "n1_mn_pct_pts":_evol_pts(mn_pct, mn_pct_n1),
            "n1_fc_pts":    _evol_pts(fc,  fc_n1),
            "n1_lc_pts":    _evol_pts(lc,  lc_n1),
            "n1_tkt_abs":   _evol_abs(tkt, tkt_n1)    if (tkt and tkt_n1) else None,
            "n1_tkt_pct":   _evol_pct(tkt, tkt_n1)    if (tkt and tkt_n1) else None,
            "n1_label":     ALL_MONTH_LABELS[ALL_MONTHS.index(n1_m)] if (n1_m in ALL_MONTHS and n1) else None,
            # M-1 coûts fixes / variables
            "evol_cf_abs":  _evol_abs(cf, cf_p)                      if has_prev else None,
            "evol_cf_pct":  _evol_pct(cf, cf_p)                      if (has_prev and cf_p > 0) else None,
            "evol_cv_abs":  _evol_abs(cv, cv_p)                      if has_prev else None,
            "evol_cv_pct":  _evol_pct(cv, cv_p)                      if (has_prev and cv_p > 0) else None,
            # N-1 coûts fixes / variables
            "n1_cf_abs":    _evol_abs(cf, cf_n1)                     if cf_n1 else None,
            "n1_cf_pct":    _evol_pct(cf, cf_n1)                     if cf_n1 else None,
            "n1_cv_abs":    _evol_abs(cv, cv_n1)                     if cv_n1 else None,
            "n1_cv_pct":    _evol_pct(cv, cv_n1)                     if cv_n1 else None,
        })

    # ── Mois disponibles pour le sélecteur ────────────────────────────────────
    available_months = [{"key": m["mois"], "label": m["label"]} for m in mois_data]
    valid_keys       = {m["mois"] for m in mois_data}

    # Par défaut (aucun paramètre URL) → mois précédent (M-1) ou dernier 2026 dispo
    _url_mois = request.args.get("mois")
    if _url_mois is None:
        _yr, _mo = int(today_m[:4]), int(today_m[5:])
        _prev_m  = f"{_yr - 1}-12" if _mo == 1 else f"{_yr}-{_mo - 1:02d}"
        if _prev_m in valid_keys:
            selected_month = _prev_m
        else:
            _avail_2026 = sorted(k for k in valid_keys if k.startswith("2026"))
            selected_month = _avail_2026[-1] if _avail_2026 else (max(valid_keys) if valid_keys else None)
    elif selected_month not in valid_keys:
        selected_month = None

    # ── CA cumulé (toujours le total global) ─────────────────────────────────
    ca_cumule = sum(m["ca"] for m in mois_data)

    # ── KPI du contexte sélectionné ──────────────────────────────────────────
    if selected_month and mois_data:
        kpi = next((m for m in mois_data if m["mois"] == selected_month), None)
        kpi_label = kpi["label"] if kpi else selected_month
        kpi_is_global = False
    else:
        # Vue globale : agréger les totaux, M-1 depuis le dernier mois disponible
        kpi_is_global = True
        kpi_label     = "Vue globale"
        if mois_data:
            ca_g   = ca_cumule
            cm_g   = sum(m["cm"]  for m in mois_data)
            cp_g   = sum(m["cp"]  for m in mois_data)
            cf_g   = sum(m["cf"]  for m in mois_data)
            cv_g   = sum(m["cv"]  for m in mois_data)
            chg_g  = sum(m["chg"] for m in mois_data)
            cmd_g  = sum(m["cmd"] for m in mois_data)
            tkt_g  = round(ca_g / cmd_g, 2) if cmd_g > 0 else None
            fc_g   = _safe_pct(cm_g,  ca_g)
            lc_g   = _safe_pct(cp_g,  ca_g)
            pc_g   = round((cm_g + cp_g) / ca_g * 100, 1) if ca_g > 0 and (cm_g > 0 or cp_g > 0) else None
            mn_g   = ca_g - cm_g - cp_g - chg_g
            mn_pct_g = round(mn_g / ca_g * 100, 1) if ca_g > 0 else None
            last   = mois_data[-1]  # M-1 référence = dernier mois
            kpi = {
                "ca":           ca_g,
                "cmd":          cmd_g,
                "cm":           cm_g,
                "cp":           cp_g,
                "cf":           cf_g,
                "cv":           cv_g,
                "chg":          chg_g,
                "tkt":          tkt_g,
                "fc":           fc_g,
                "lc":           lc_g,
                "pc":           pc_g,
                "mn":           mn_g,
                "mn_pct":       mn_pct_g,
                # M-1 repris du dernier mois
                "evol_ca":      last.get("evol_ca"),
                "evol_ca_abs":  last.get("evol_ca_abs"),
                "evol_tkt":     last.get("evol_tkt"),
                "evol_tkt_abs": last.get("evol_tkt_abs"),
                "evol_fc":      last.get("evol_fc"),
                "evol_cm_abs":  last.get("evol_cm_abs"),
                "evol_lc":      last.get("evol_lc"),
                "evol_cp_abs":  last.get("evol_cp_abs"),
                "evol_chg_abs": last.get("evol_chg_abs"),
                "evol_mn_pct":  last.get("evol_mn_pct"),
                "evol_mn_abs":  last.get("evol_mn_abs"),
                "evol_mn_rel":  last.get("evol_mn_rel"),
                # N-1 global : CA 2025 vs CA 2026 (tous mois)
                "n1_ca":        sum(m["ca"] for m in mois_data if m["annee"] == "2025") or None,
                "n1_ca_abs":    None,  # calculé après
                "n1_ca_pct":    None,
                "n1_mn":        None,
                "n1_mn_abs":    None,
                "n1_mn_pct":    None,
                "n1_mn_pct_pts":None,
                "n1_fc_pts":    None,
                "n1_lc_pts":    None,
                "n1_tkt_abs":   None,
                "n1_tkt_pct":   None,
                "n1_label":     "2025",
                # cf/cv évolutions — non calculées en vue globale
                "evol_cf_abs":  None, "evol_cf_pct":  None,
                "evol_cv_abs":  None, "evol_cv_pct":  None,
                "n1_cf_abs":    None, "n1_cf_pct":    None,
                "n1_cv_abs":    None, "n1_cv_pct":    None,
            }
            # Compléter n1 global : CA 2026 vs CA 2025
            if kpi and kpi.get("n1_ca"):
                ca_2026 = sum(m["ca"] for m in mois_data if m["annee"] == "2026")
                ca_2025 = kpi["n1_ca"]
                kpi["n1_ca_abs"] = ca_2026 - ca_2025
                kpi["n1_ca_pct"] = round((ca_2026 - ca_2025) / ca_2025 * 100, 1) if ca_2025 else None
        else:
            kpi = None

    # ── Classement CA parmi la même marque ───────────────────────────────────
    all_restos = _get_all_restaurants(data)
    brand_restos = [r for r in all_restos if r.get("brand") == brand]
    brand_ca_rank = []
    for r in brand_restos:
        r_ca_raw = data.get(r["id"], {})
        if selected_month:
            r_ca_val = r_ca_raw.get(selected_month) or 0
        else:
            r_ca_val = sum((r_ca_raw.get(mo) or 0) for mo in ALL_MONTHS)
        brand_ca_rank.append({"id": r["id"], "ca": r_ca_val})
    brand_ca_rank.sort(key=lambda x: x["ca"], reverse=True)
    total_brand   = len(brand_ca_rank)
    ranking_pos   = next((i + 1 for i, r in enumerate(brand_ca_rank) if r["id"] == rid), None)
    ranking       = {"pos": ranking_pos, "total": total_brand, "brand": brand} if ranking_pos else None

    # ── Dernier mois (pour hero section) ─────────────────────────────────────
    dernier = mois_data[-1] if mois_data else None

    # ── Meilleur / Pire mois ──────────────────────────────────────────────────
    meilleur_mois = max(mois_data, key=lambda x: x["ca"]) if mois_data else None
    pire_mois     = min(mois_data, key=lambda x: x["ca"]) if mois_data else None

    # ── Moyennes ──────────────────────────────────────────────────────────────
    def _avg(field):
        vals = [m[field] for m in mois_data if m.get(field) is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    moyennes = {
        "ca":  round(sum(m["ca"] for m in mois_data) / len(mois_data)) if mois_data else None,
        "tkt": _avg("tkt"),
        "mb":  _avg("mb"),
        "fc":  _avg("fc"),
        "lc":  _avg("lc"),
        "pc":  _avg("pc"),
    }

    # ── Tendance globale CA (3 derniers mois) ─────────────────────────────────
    if len(mois_data) >= 3:
        last3 = [m["ca"] for m in mois_data[-3:]]
        if last3[0] < last3[1] < last3[2]:
            tendance = "hausse"
        elif last3[0] > last3[1] > last3[2]:
            tendance = "baisse"
        else:
            tendance = "stable"
    else:
        tendance = "stable"

    # ── Tableau trié décroissant ──────────────────────────────────────────────
    tableau = list(reversed(mois_data))

    # ── Données Chart.js — 2 séries : 2025 et 2026 ───────────────────────────
    # On aligne par mois (Jan→Déc) pour comparer les deux années
    chart_labels   = [l for l in MONTH_LABELS_2025]  # Jan–Déc (court)
    chart_labels_s = ["Jan","Fév","Mar","Avr","Mai","Juin","Juil","Août","Sep","Oct","Nov","Déc"]
    data_by_month  = {m["mois"]: m["ca"] for m in mois_data}
    chart_ca_2025  = [data_by_month.get(m, 0) for m in MONTHS_2025]
    chart_ca_2026  = [data_by_month.get(m, 0) for m in MONTHS]
    # Série linéaire complète (tous mois confondus, pour compatibilité)
    chart_ca       = [m["ca"] for m in mois_data]
    chart_labels_full = [m["label"] for m in mois_data]

    # ── CA cumulé 2025 (pour comparaison globale) ─────────────────────────────
    ca_cumule_2025 = sum(data_by_month.get(m, 0) for m in MONTHS_2025)
    ca_cumule_2026 = sum(data_by_month.get(m, 0) for m in MONTHS)

    # ── TOP5 / FLOP5 produits ─────────────────────────────────────────────────
    prod_raw = []

    # Source 1 : produits saisis manuellement (data["produits"][rid])
    for m, items in data.get("produits", {}).get(rid, {}).items():
        if not isinstance(items, list):
            continue
        for item in items:
            nom = str(item.get("nom", "")).strip()
            quantite = _product_number(item.get("quantite"))
            prix_u   = _product_number(item.get("prix_unitaire"))
            if not nom or quantite <= 0:
                continue
            prod_raw.append({
                "nom":      nom,
                "quantite": round(quantite, 2),
                "ca":       round(quantite * max(prix_u, 0), 2),
                "month":    m,
            })

    # Source 2 : imports Excel (data["ventes_produits"][month][brand]["restaurants"][rid])
    for m, by_brand in data.get("ventes_produits", {}).items():
        if not isinstance(by_brand, dict):
            continue
        articles = by_brand.get(brand, {}).get("restaurants", {}).get(rid, {})
        if not isinstance(articles, dict):
            continue
        for article in articles.values():
            if not isinstance(article, dict):
                continue
            nom    = str(article.get("article", "")).strip()
            ventes = _product_number(article.get("ventes"))
            q      = _product_number(article.get("quantite")) if article.get("quantite") is not None else 0.0
            if not nom or (ventes <= 0 and q <= 0):
                continue
            prod_raw.append({
                "nom":      nom,
                "quantite": round(q, 2),
                "ca":       round(ventes, 2),
                "month":    m,
            })

    # Filtre sur le mois sélectionné
    if selected_month:
        prod_raw = [r for r in prod_raw if r["month"] == selected_month]

    # Agrégation par nom de produit
    prod_agg: dict = {}
    for r in prod_raw:
        key = r["nom"]
        bucket = prod_agg.setdefault(key, {"nom": key, "quantite": 0.0, "ca": 0.0})
        bucket["quantite"] += r["quantite"]
        bucket["ca"]       += r["ca"]

    all_products = [
        {"nom": v["nom"], "quantite": round(v["quantite"], 2), "ca": round(v["ca"], 2)}
        for v in prod_agg.values()
        if v["quantite"] > 0 or v["ca"] > 0
    ]

    # TOP5 / FLOP5 quantité (uniquement produits avec qty > 0)
    by_qty     = sorted([p for p in all_products if p["quantite"] > 0],
                        key=lambda x: x["quantite"], reverse=True)
    top5_qty   = by_qty[:5]
    flop5_qty  = list(reversed(by_qty[-5:])) if by_qty else []

    # TOP5 / FLOP5 CA (uniquement produits avec ca > 0)
    by_ca      = sorted([p for p in all_products if p["ca"] > 0],
                        key=lambda x: x["ca"], reverse=True)
    top5_ca    = by_ca[:5]
    flop5_ca   = list(reversed(by_ca[-5:])) if by_ca else []

    products_data = {
        "top5_qty":  top5_qty,
        "flop5_qty": flop5_qty,
        "top5_ca":   top5_ca,
        "flop5_ca":  flop5_ca,
        "has_data":  bool(all_products),
    }

    # ── Benchmarking interne (comparaison enseigne) ───────────────────────────
    bench_months = [selected_month] if selected_month else [m["mois"] for m in mois_data]

    def _bench_kpis(r_id, months):
        """Agrège CA, FC, LC pour un restaurant sur une liste de mois."""
        r_ca  = data.get(r_id, {})
        r_cm  = data.get("cout_matieres",  {}).get(r_id, {})
        r_cp  = data.get("cout_personnel", {}).get(r_id, {})
        r_cmd = data.get("commandes",      {}).get(r_id, {})
        ca_t = cm_t = cp_t = cmd_t = 0.0
        for m in months:
            ca_v = r_ca.get(m) or 0
            if ca_v <= 0:
                continue
            ca_t  += ca_v
            cm_t  += r_cm.get(m) or 0
            cp_t  += r_cp.get(m) or 0
            cmd_t += r_cmd.get(m) or 0
        if ca_t <= 0:
            return None
        return {
            "ca":  round(ca_t),
            "fc":  round(cm_t / ca_t * 100, 1) if cm_t > 0 else None,
            "lc":  round(cp_t / ca_t * 100, 1) if cp_t > 0 else None,
        }

    bench_rows = []
    for r in brand_restos:
        bk = _bench_kpis(r["id"], bench_months)
        if bk:
            bk["id"]   = r["id"]
            bk["name"] = r["name"]
            bench_rows.append(bk)

    def _bench_stat(field, higher_is_better=True):
        """Calcule min/avg/max/best pour un champ sur bench_rows."""
        vals = [(b["name"], b[field]) for b in bench_rows if b.get(field) is not None]
        if not vals:
            return None
        v_list = [v for _, v in vals]
        avg  = round(sum(v_list) / len(v_list), 1 if field != "ca" else 0)
        mn   = min(v_list)
        mx   = max(v_list)
        best_name, best_val = (max if higher_is_better else min)(vals, key=lambda x: x[1])
        me   = kpi.get(field) if kpi else None
        gap  = round(me - avg, 1 if field != "ca" else 0) if me is not None else None
        gap_pct = round((me - avg) / avg * 100, 1) if (me is not None and avg and field == "ca") else None
        # Position sur l'échelle min–max (%)
        pos_pct = round((me - mn) / (mx - mn) * 100) if (me is not None and mx != mn) else (50 if me is not None else None)
        better  = (gap > 0) if (higher_is_better and gap is not None) else (gap < 0 if gap is not None else None)
        return {
            "me":        me,
            "avg":       avg,
            "best":      best_val,
            "best_name": best_name,
            "min":       mn,
            "max":       mx,
            "gap":       gap,
            "gap_pct":   gap_pct,
            "pos_pct":   pos_pct,
            "better":    better,
            "n":         len(vals),
        }

    if len(bench_rows) >= 2:
        bench_data = {
            "has_data":  True,
            "brand":     brand,
            "n":         len(bench_rows),
            "ca":        _bench_stat("ca",  higher_is_better=True),
            "fc":        _bench_stat("fc",  higher_is_better=False),
            "lc":        _bench_stat("lc",  higher_is_better=False),
        }
    else:
        bench_data = {"has_data": False, "brand": brand, "n": len(bench_rows)}

    return render_template(
        "restaurant_profil.html",
        export_mode=export_mode,
        resto=resto,
        color=color,
        dernier=dernier,
        mois_data=mois_data,
        tableau=tableau,
        meilleur_mois=meilleur_mois,
        pire_mois=pire_mois,
        moyennes=moyennes,
        tendance=tendance,
        chart_labels=chart_labels_full,
        chart_ca=chart_ca,
        chart_labels_short=chart_labels_s,
        chart_ca_2025=chart_ca_2025,
        chart_ca_2026=chart_ca_2026,
        brand_colors=BRAND_COLORS,
        nb_mois=len(mois_data),
        kpi=kpi,
        kpi_label=kpi_label,
        kpi_is_global=kpi_is_global,
        available_months=available_months,
        selected_month=selected_month,
        ca_cumule=ca_cumule,
        ca_cumule_2025=ca_cumule_2025,
        ca_cumule_2026=ca_cumule_2026,
        ranking=ranking,
        products_data=products_data,
        bench_data=bench_data,
    )


@app.route("/import-produits")
@login_required
def import_produits():
    data = load_data()
    pending = data.get("_pending_import")
    return render_template(
        "import_produits.html",
        available_brands=list(BRAND_COLORS.keys()),
        brand_colors=BRAND_COLORS,
        months=MONTHS,
        month_labels=MONTH_LABELS,
        months_with_labels=list(zip(MONTHS, MONTH_LABELS)),
        current_month=_current_month_key(),
        pending=pending,
    )


@app.route("/import-produits/preview", methods=["POST"])
@login_required
def import_produits_preview():
    data = load_data()
    all_restos = _get_all_restaurants(data)

    brand = request.form.get("brand", "").strip()
    month = request.form.get("month", "").strip()
    file = request.files.get("excel_file")

    errors = []
    if brand not in BRAND_COLORS:
        errors.append(f"Marque invalide : « {brand} ». Valeurs acceptées : {', '.join(BRAND_COLORS.keys())}.")
    if month not in MONTHS:
        errors.append(f"Mois invalide : « {month} ».")
    if file is None or not file.filename:
        errors.append("Aucun fichier sélectionné.")
    elif not file.filename.lower().endswith(".xlsx"):
        errors.append("Seuls les fichiers .xlsx sont acceptés.")

    if errors:
        return jsonify({"ok": False, "errors": errors}), 400

    brand_restos = [r for r in all_restos if r.get("brand") == brand]
    try:
        imported = _import_product_sales_xlsx(file, brand, month, brand_restos)
    except (ValueError, zipfile.BadZipFile) as exc:
        return jsonify({"ok": False, "errors": [str(exc)]}), 400

    resto_map = {r["id"]: r for r in all_restos}
    preview_rows = []
    for resto_id, articles in imported["restaurants"].items():
        resto = resto_map.get(resto_id, {})
        for article_key, article in articles.items():
            quantite = article.get("quantite", 0) or 0
            ventes = article.get("ventes", 0) or 0
            prix_unitaire = round(ventes / quantite, 2) if quantite > 0 else None
            preview_rows.append({
                "restaurant": resto.get("name", resto_id),
                "brand": resto.get("brand", brand),
                "article": article.get("article", ""),
                "categorie": article.get("categorie", "Non classé"),
                "quantite": quantite,
                "prix_unitaire": prix_unitaire,
                "ventes": ventes,
            })

    preview_rows.sort(key=lambda r: (-r["ventes"], -r["quantite"], r["restaurant"], r["article"]))

    data["_pending_import"] = {
        "brand": brand,
        "month": month,
        "month_label": MONTH_LABELS[MONTHS.index(month)],
        "filename": file.filename,
        "imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "restaurants": imported["restaurants"],
        "stats": imported["stats"],
    }
    _save_data(data)

    return jsonify({
        "ok": True,
        "brand": brand,
        "month": month,
        "month_label": MONTH_LABELS[MONTHS.index(month)],
        "filename": file.filename,
        "stats": imported["stats"],
        "headers_found": imported["headers_found"],
        "rows": preview_rows,
    })


@app.route("/import-produits/confirm", methods=["POST"])
@login_required
def import_produits_confirm():
    data = load_data()
    pending = data.get("_pending_import")
    if not pending:
        flash("Aucun import en attente à confirmer.", "error")
        return redirect(url_for("import_produits"))

    brand = pending["brand"]
    month = pending["month"]

    data.setdefault("ventes_produits", {}).setdefault(month, {})[brand] = {
        "restaurants": pending["restaurants"],
    }
    data.setdefault("ventes_produits_imports", {}).setdefault(month, {})[brand] = {
        "filename": pending["filename"],
        "imported_at": pending["imported_at"],
        "stats": pending["stats"],
    }
    data.pop("_pending_import", None)
    _save_data(data)

    stats = pending["stats"]
    flash(
        f"Import validé — {brand} · {pending['month_label']} : "
        f"{stats['rows_kept']} lignes conservées, "
        f"{stats['rows_negative_skipped']} négatives supprimées, "
        f"{stats['rows_unknown_restaurant']} restaurants non reconnus.",
        "success",
    )
    return redirect(url_for("produits"))


@app.route("/import-produits/cancel", methods=["POST"])
@login_required
def import_produits_cancel():
    data = load_data()
    data.pop("_pending_import", None)
    _save_data(data)
    return redirect(url_for("import_produits"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        user_data = USERS.get(username)
        if user_data and user_data["password"] == password:
            login_user(User(username), remember=True)
            return redirect(request.args.get("next") or url_for("dashboard"))
        error = "Identifiants incorrects."
    return render_template("login.html", error=error)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


if __name__ == "__main__":
    import os
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)
