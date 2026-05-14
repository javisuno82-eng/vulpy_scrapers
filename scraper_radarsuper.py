"""
scraper_radarsuper.py
=====================
Scraper diario de radarsuper.com → Supabase (gastos_app)

Estrategia:
  - Recorre por categorías (más limpio que paginación general)
  - Extrae: nombre, precio, precio/kg o precio/L, categoría, subcategoría
  - Cadenas cubiertas: Mercadona y Carrefour
  - Mapea categorías de RadarSuper → categorías de gastos_app
  - Fuzzy matching contra tabla `productos` de Supabase
  - Upsert en tabla `precios` con fuente='scraper'

Uso:
  python scraper_radarsuper.py                        # todo el catálogo
  python scraper_radarsuper.py --cadena mercadona     # solo Mercadona
  python scraper_radarsuper.py --cadena carrefour     # solo Carrefour
  python scraper_radarsuper.py --test                 # 3 categorías, modo test

Configuración:
  .env con SUPABASE_URL, SUPABASE_KEY, CIUDAD_SCRAPER
"""

import os
import re
import time
import logging
import argparse
from datetime import date
from typing import Optional

import requests
from bs4 import BeautifulSoup
from thefuzz import fuzz
from dotenv import load_dotenv
from supabase import create_client, Client

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
load_dotenv()

SUPABASE_URL   = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")
CIUDAD_SCRAPER = os.getenv("CIUDAD_SCRAPER", "Nacional")

BASE_URL       = "https://radarsuper.com"

FUZZY_THRESHOLD = 72

SLEEP_ENTRE_PAGINAS    = 2.0
SLEEP_ENTRE_CATEGORIAS = 3.0

# Cadenas disponibles en RadarSuper
CADENAS = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
}

# Mapeo categorías RadarSuper → categorías gastos_app
CATEGORIA_MAP = {
    "aceite-especias-salsas":      "condimentos",
    "agua-refrescos":              "bebidas",
    "aperitivos":                  "snacks",
    "arroz-legumbres-pasta":       "pasta",
    "azucar-caramelos-chocolate":  "dulces",
    "bebe":                        "higiene",
    "bodega":                      "bebidas",
    "cacao-cafe-e-infusiones":     "café",
    "carne":                       "carne",
    "cereales-galletas":           "galletas",
    "charcuteria-quesos":          "charcutería",
    "congelados":                  "congelados",
    "conservas-caldos-cremas":     "conservas",
    "cuidado-cabello":             "higiene",
    "cuidado-facial-corporal":     "higiene",
    "fitoterapia-parafarmacia":    "farmacia",
    "fruta-verdura":               "fruta",
    "huevos-leche-mantequilla":    "lácteos",
    "limpieza-hogar":              "limpieza",
    "maquillaje":                  "higiene",
    "marisco-pescado":             "pescado",
    "mascotas":                    "mascotas",
    "panaderia-pasteleria":        "pan",
    "pizzas-platos-preparados":    "preparados",
    "postres-yogures":             "lácteos",
    "zumos":                       "bebidas",
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper_radarsuper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; gastos_app-scraper/1.0; "
        "+https://github.com/tu_usuario/gastos_app)"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
}


# ─────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_KEY en el .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def cargar_tiendas(sb: Client) -> dict:
    res = sb.table("tiendas").select("id, nombre").execute()
    return {r["nombre"]: r["id"] for r in res.data}


def cargar_productos(sb: Client) -> list[dict]:
    res = sb.table("productos").select("id, nombre, marca, categoria").execute()
    return res.data


def buscar_producto_fuzzy(nombre: str, productos: list[dict]) -> tuple[Optional[dict], int]:
    mejor = None
    mejor_score = 0
    nombre_lower = nombre.lower()
    for p in productos:
        score = fuzz.token_set_ratio(nombre_lower, p["nombre"].lower())
        if score > mejor_score:
            mejor_score = score
            mejor = p
    if mejor_score >= FUZZY_THRESHOLD:
        return mejor, mejor_score
    return None, mejor_score


def insertar_producto(sb: Client, nombre: str, categoria: str, subcategoria: str, tienda_id: str) -> Optional[str]:
    try:
        res = sb.table("productos").insert({
            "nombre":        nombre,
            "categoria":     categoria,
            "subcategoria":  subcategoria,
            "tienda_origen": tienda_id,
            "verificado":    False,
        }).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        log.error(f"Error insertando producto '{nombre}': {e}")
    return None


def upsert_precio(sb: Client, producto_id: str, tienda_id: str, precio: float):
    try:
        sb.table("precios").upsert({
            "producto_id": producto_id,
            "tienda_id":   tienda_id,
            "ciudad":      CIUDAD_SCRAPER,
            "precio":      precio,
            "fecha":       str(date.today()),
            "fuente":      "radarsuper",
        }, on_conflict="producto_id,tienda_id,ciudad,fecha").execute()
    except Exception as e:
        log.error(f"Error upsert precio producto_id={producto_id}: {e}")


# ─────────────────────────────────────────────
# SCRAPING
# ─────────────────────────────────────────────
def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"Error descargando {url}: {e}")
        return None


def extraer_categorias(cadena_slug: str) -> list[dict]:
    """
    Extrae todas las categorías y subcategorías del catálogo de una cadena.
    Devuelve lista de dicts con slug, nombre, url, categoria_padre.
    """
    url = f"{BASE_URL}/{cadena_slug}"
    soup = fetch(url)
    if not soup:
        return []

    categorias = []
    # Los links de categoría siguen el patrón /{cadena}/c/{nombre}-{id}
    patron = re.compile(rf"/{cadena_slug}/c/([\w-]+)-(\d+)")

    vistos = set()
    for a in soup.find_all("a", href=patron):
        href = a["href"]
        if href in vistos:
            continue
        vistos.add(href)

        match = patron.search(href)
        if not match:
            continue

        slug_cat = match.group(1)
        nombre   = a.get_text(strip=True)
        # Limpiar el número de productos del nombre "Cereales42" → "Cereales"
        nombre   = re.sub(r"\d+$", "", nombre).strip()

        if not nombre or len(nombre) < 3:
            continue

        # Inferir categoría padre del slug
        cat_padre = None
        for k in CATEGORIA_MAP:
            if slug_cat.startswith(k.replace("-", "")):
                cat_padre = CATEGORIA_MAP[k]
                break

        categorias.append({
            "slug":    slug_cat,
            "nombre":  nombre,
            "url":     BASE_URL + href,
            "cat_app": cat_padre or "general",
        })

    log.info(f"  → {len(categorias)} categorías encontradas en {cadena_slug}")
    return categorias


def parsear_productos_pagina(soup: BeautifulSoup, cadena_slug: str) -> list[dict]:
    """
    Extrae productos de una página de listado de RadarSuper.
    Devuelve lista de dicts con nombre, precio, precio_kg, url_producto.
    """
    productos = []
    patron = re.compile(rf"/{cadena_slug}/p/([\w-]+)")
    vistos = set()

    for a in soup.find_all("a", href=patron):
        href = a["href"]
        if href in vistos:
            continue
        vistos.add(href)

        texto = a.get_text(" ", strip=True)

        # Nombre: primera línea de texto antes del precio
        lineas = [l.strip() for l in texto.split("\n") if l.strip()]
        if not lineas:
            continue
        nombre = lineas[0]
        if not nombre or len(nombre) < 4:
            continue

        # Precio principal: patrón "1,14 €"
        precio_match = re.search(r"(\d+[.,]\d+)\s*€", texto)
        if not precio_match:
            continue
        precio = float(precio_match.group(1).replace(",", "."))

        # Precio por kg/L: patrón "2,30 €/kg" o "1,75 €/L"
        precio_kg_match = re.search(r"(\d+[.,]\d+)\s*€\s*/\s*(kg|L|ud)", texto)
        precio_kg = None
        unidad_precio = None
        if precio_kg_match:
            precio_kg    = float(precio_kg_match.group(1).replace(",", "."))
            unidad_precio = precio_kg_match.group(2)

        productos.append({
            "nombre":       nombre,
            "precio":       precio,
            "precio_kg":    precio_kg,
            "unidad_precio": unidad_precio,
            "url":          BASE_URL + href if href.startswith("/") else href,
        })

    return productos


def get_total_paginas_categoria(soup: BeautifulSoup) -> int:
    """Extrae el número total de páginas de una categoría."""
    try:
        # Busca "Página X de Y"
        texto = soup.get_text(" ")
        match = re.search(r"Página\s+\d+\s+de\s+(\d+)", texto)
        if match:
            return int(match.group(1))
        # Busca links de paginación
        paginas = soup.find_all("a", href=re.compile(r"\?page=\d+"))
        nums = [int(re.search(r"page=(\d+)", a["href"]).group(1))
                for a in paginas if re.search(r"page=(\d+)", a["href"])]
        if nums:
            return max(nums)
    except Exception:
        pass
    return 1


def scrape_categoria(
    sb: Client,
    cadena_slug: str,
    categoria: dict,
    tienda_id: str,
    productos_db: list[dict],
    stats: dict,
    modo_test: bool = False,
):
    """Procesa todas las páginas de una categoría."""
    url_base    = categoria["url"]
    cat_app     = categoria["cat_app"]
    subcat_nombre = categoria["nombre"]

    log.info(f"  📂 {subcat_nombre} ({cat_app}) — {url_base}")

    # Primera página
    soup = fetch(url_base)
    if not soup:
        return

    total_pags = get_total_paginas_categoria(soup)
    if modo_test:
        total_pags = min(total_pags, 1)

    for page_num in range(1, total_pags + 1):
        url = f"{url_base}?page={page_num}" if page_num > 1 else url_base
        soup_pag = soup if page_num == 1 else fetch(url)
        if not soup_pag:
            continue

        items = parsear_productos_pagina(soup_pag, cadena_slug)
        log.info(f"     Página {page_num}/{total_pags} → {len(items)} productos")

        for item in items:
            stats["procesados"] += 1
            nombre = item["nombre"]
            precio = item["precio"]

            # Fuzzy match
            match, score = buscar_producto_fuzzy(nombre, productos_db)

            if match:
                producto_id = match["id"]
                stats["actualizados"] += 1
                log.debug(f"Match ({score}%): '{nombre}' → '{match['nombre']}'")
            else:
                # Producto nuevo
                producto_id = insertar_producto(sb, nombre, cat_app, subcat_nombre, tienda_id)
                if not producto_id:
                    stats["errores"] += 1
                    continue
                productos_db.append({
                    "id":        producto_id,
                    "nombre":    nombre,
                    "marca":     None,
                    "categoria": cat_app,
                })
                stats["nuevos"] += 1
                log.info(f"Nuevo producto (score={score}%): '{nombre}'")

            upsert_precio(sb, producto_id, tienda_id, precio)

        time.sleep(SLEEP_ENTRE_PAGINAS)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main(cadenas_seleccionadas: list[str], modo_test: bool = False):
    log.info("🛒 Iniciando scraper RadarSuper → Supabase")

    sb = get_supabase()
    log.info("✅ Conectado a Supabase")

    tiendas_map  = cargar_tiendas(sb)
    productos_db = cargar_productos(sb)
    log.info(f"📦 {len(productos_db)} productos y {len(tiendas_map)} tiendas cargados")

    stats = {
        "procesados":  0,
        "actualizados": 0,
        "nuevos":      0,
        "errores":     0,
    }

    for cadena_slug, cadena_nombre in CADENAS.items():
        if cadena_slug not in cadenas_seleccionadas:
            continue

        tienda_id = tiendas_map.get(cadena_nombre)
        if not tienda_id:
            log.warning(f"Tienda '{cadena_nombre}' no encontrada en Supabase — ¿has ejecutado el SQL?")
            continue

        log.info(f"\n🏪 Procesando {cadena_nombre}...")

        categorias = extraer_categorias(cadena_slug)

        if modo_test:
            categorias = categorias[:3]
            log.info(f"Modo test: procesando solo {len(categorias)} categorías")

        for i, categoria in enumerate(categorias, 1):
            log.info(f"\n  [{i}/{len(categorias)}] {categoria['nombre']}")
            scrape_categoria(
                sb, cadena_slug, categoria,
                tienda_id, productos_db, stats, modo_test
            )
            time.sleep(SLEEP_ENTRE_CATEGORIAS)

    # Resumen
    log.info("\n" + "=" * 60)
    log.info("✅ Scraper RadarSuper finalizado")
    log.info(f"   Procesados  : {stats['procesados']}")
    log.info(f"   Actualizados: {stats['actualizados']}")
    log.info(f"   Nuevos      : {stats['nuevos']}")
    log.info(f"   Errores     : {stats['errores']}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper RadarSuper → Supabase")
    parser.add_argument(
        "--cadena",
        choices=list(CADENAS.keys()) + ["todas"],
        default="todas",
        help="Cadena a procesar (default: todas)"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Modo test: solo 3 categorías, 1 página cada una"
    )
    args = parser.parse_args()

    if args.cadena == "todas":
        seleccion = list(CADENAS.keys())
    else:
        seleccion = [args.cadena]

    main(cadenas_seleccionadas=seleccion, modo_test=args.test)
