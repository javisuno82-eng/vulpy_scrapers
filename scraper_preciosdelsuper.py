"""
scraper_preciosdelsuper.py
==========================
Scraper diario de preciosdelsuper.es → Supabase (gastos_app)

Qué hace:
  1. Recorre /cambios?page=N extrayendo productos y precios
  2. Por cada producto, busca coincidencia en la tabla `productos` de Supabase
     usando fuzzy matching (thefuzz) sobre el nombre
  3. Si encuentra match con similitud > umbral → inserta en `precios`
  4. Si no encuentra match → inserta el producto nuevo en `productos` y luego el precio

Uso:
  python scraper_preciosdelsuper.py                  # todas las páginas
  python scraper_preciosdelsuper.py --pages 1 5      # páginas 1 a 5 (test)
  python scraper_preciosdelsuper.py --solo-cambios    # solo productos con cambio de precio

Configuración:
  Crea un archivo .env con:
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_KEY=tu_service_role_key   ← usa service_role, no anon
    CIUDAD_SCRAPER=Nacional            ← ciudad que se asignará a los precios scrapeados
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

SUPABASE_URL    = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY    = os.getenv("SUPABASE_KEY", "")
CIUDAD_SCRAPER  = os.getenv("CIUDAD_SCRAPER", "Nacional")

BASE_URL        = "https://preciosdelsuper.es"
CAMBIOS_URL     = f"{BASE_URL}/cambios"

# Umbral de similitud para considerar que es el mismo producto (0-100)
FUZZY_THRESHOLD = 70

# Pausa entre peticiones (segundos) — respetuoso con el servidor
SLEEP_ENTRE_PAGINAS   = 2.0
SLEEP_ENTRE_PRODUCTOS = 0.5

# Mapeo de nombres de supermercado en la web → nombre en tu tabla `tiendas`
TIENDA_MAP = {
    "MERCADONA":     "Mercadona",
    "CARREFOUR":     "Carrefour",
    "DIA":           "Dia",
    "ALCAMPO":       "Alcampo",
    "EL CORTE INGLÉS": "El Corte Inglés",
    "CONSUM":        "Consum",
    "GADIS":         "Gadis",
    "AHORRAMAS":     "Ahorramas",
    "FROIZ":         "Froiz",
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_KEY en el .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def cargar_tiendas(sb: Client) -> dict:
    """Devuelve dict {nombre_normalizado: uuid}"""
    res = sb.table("tiendas").select("id, nombre").execute()
    return {r["nombre"]: r["id"] for r in res.data}


def cargar_productos(sb: Client) -> list[dict]:
    """Carga todos los productos para fuzzy matching en memoria."""
    res = sb.table("productos").select("id, nombre, marca, tienda_origen").execute()
    return res.data


def buscar_producto_fuzzy(nombre_scraped: str, productos: list[dict]) -> Optional[dict]:
    """Busca el producto más similar usando fuzzy matching."""
    mejor = None
    mejor_score = 0
    nombre_lower = nombre_scraped.lower()

    for p in productos:
        score = fuzz.token_set_ratio(nombre_lower, p["nombre"].lower())
        if score > mejor_score:
            mejor_score = score
            mejor = p

    if mejor_score >= FUZZY_THRESHOLD:
        return mejor, mejor_score
    return None, mejor_score


def insertar_producto(sb: Client, nombre: str, tienda_id: str) -> Optional[str]:
    """Inserta un producto nuevo y devuelve su UUID."""
    try:
        res = sb.table("productos").insert({
            "nombre": nombre,
            "tienda_origen": tienda_id,
            "verificado": False,   # pendiente de validación manual
        }).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        log.error(f"Error insertando producto '{nombre}': {e}")
    return None


def upsert_precio(sb: Client, producto_id: str, tienda_id: str, precio: float, ciudad: str):
    """Inserta o actualiza el precio. Si ya existe para hoy, actualiza."""
    try:
        sb.table("precios").upsert({
            "producto_id": producto_id,
            "tienda_id":   tienda_id,
            "ciudad":      ciudad,
            "precio":      precio,
            "fecha":       str(date.today()),
            "fuente":      "scraper",
        }, on_conflict="producto_id,tienda_id,ciudad,fecha").execute()
    except Exception as e:
        log.error(f"Error insertando precio producto_id={producto_id}: {e}")


# ─────────────────────────────────────────────
# SCRAPING
# ─────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; gastos_app-scraper/1.0; "
        "+https://github.com/tu_usuario/gastos_app)"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
}


def fetch_page(url: str) -> Optional[BeautifulSoup]:
    """Descarga una página y devuelve el BeautifulSoup."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"Error descargando {url}: {e}")
        return None


def get_total_paginas(soup: BeautifulSoup) -> int:
    """Extrae el número total de páginas del paginador."""
    try:
        # Busca el enlace "Última" que tiene el número de página más alto
        ultima = soup.select_one('a[href*="page="]:last-of-type')
        if ultima:
            match = re.search(r"page=(\d+)", ultima["href"])
            if match:
                return int(match.group(1))
    except Exception:
        pass
    return 1


def parsear_producto_de_card(card) -> Optional[dict]:
    """
    Extrae datos de una card de producto en /cambios.
    Devuelve dict con nombre, precio, tienda, url o None si falla.
    """
    try:
        # Nombre del producto
        nombre_el = card.select_one("h2, h3, .product-name, p")
        if not nombre_el:
            return None
        nombre = nombre_el.get_text(strip=True)
        if not nombre or len(nombre) < 3:
            return None

        # Precio — busca patrones como "1,95€" o "1.95€"
        texto_card = card.get_text(" ", strip=True)
        precio_match = re.search(r"(\d+[.,]\d+)\s*€", texto_card)
        if not precio_match:
            return None
        precio_str = precio_match.group(1).replace(",", ".")
        precio = float(precio_str)

        # Tienda — busca el alt de la imagen de la tienda
        img_tienda = card.select_one("img[alt]")
        tienda_raw = ""
        if img_tienda:
            tienda_raw = img_tienda.get("alt", "").strip().upper()

        # URL del producto
        link = card.select_one("a[href*='/producto/']")
        url_producto = ""
        if link:
            url_producto = BASE_URL + link["href"] if link["href"].startswith("/") else link["href"]

        return {
            "nombre":   nombre,
            "precio":   precio,
            "tienda":   tienda_raw,
            "url":      url_producto,
        }
    except Exception as e:
        log.debug(f"Error parseando card: {e}")
        return None


def parsear_pagina_cambios(soup: BeautifulSoup) -> list[dict]:
    """Extrae todos los productos de una página de /cambios."""
    productos = []

    # Los productos están en links con /producto/ en el href
    cards = soup.select("a[href*='/producto/']")

    # Agrupamos por href para no repetir (cada producto puede tener varios links)
    vistos = set()
    for card in cards:
        href = card.get("href", "")
        if href in vistos:
            continue
        vistos.add(href)

        # Extraemos datos del contexto del link
        nombre_el = card.select_one("p, h2, h3, span")
        if not nombre_el:
            continue
        nombre = nombre_el.get_text(strip=True)
        if not nombre or len(nombre) < 5:
            continue

        # Precio en el texto del link
        texto = card.get_text(" ", strip=True)
        precio_match = re.search(r"(\d+[.,]\d+)\s*€", texto)
        if not precio_match:
            continue
        precio = float(precio_match.group(1).replace(",", "."))

        # Tienda — en el alt de la imagen dentro del link
        img = card.select_one("img[alt]")
        tienda_raw = img.get("alt", "").strip().upper() if img else ""

        url_producto = BASE_URL + href if href.startswith("/") else href

        productos.append({
            "nombre":  nombre,
            "precio":  precio,
            "tienda":  tienda_raw,
            "url":     url_producto,
        })

    return productos


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main(page_inicio: int = 1, page_fin: Optional[int] = None, solo_cambios: bool = False):
    log.info("🛒 Iniciando scraper preciosdelsuper.es → Supabase")

    # Conectar Supabase
    sb = get_supabase()
    log.info("✅ Conectado a Supabase")

    # Cargar catálogos en memoria
    tiendas_map  = cargar_tiendas(sb)
    productos_db = cargar_productos(sb)
    log.info(f"📦 Cargados {len(productos_db)} productos y {len(tiendas_map)} tiendas")

    # Determinar número total de páginas
    primera_soup = fetch_page(CAMBIOS_URL)
    if not primera_soup:
        log.error("No se pudo acceder a preciosdelsuper.es")
        return

    total_paginas = get_total_paginas(primera_soup)
    if page_fin is None:
        page_fin = total_paginas

    log.info(f"📄 Páginas a procesar: {page_inicio} → {page_fin} (total disponible: {total_paginas})")

    # Contadores
    procesados   = 0
    insertados   = 0
    actualizados = 0
    nuevos_prod  = 0
    sin_tienda   = 0

    # Iterar páginas
    for page_num in range(page_inicio, page_fin + 1):
        url = f"{CAMBIOS_URL}?page={page_num}" if page_num > 1 else CAMBIOS_URL
        log.info(f"📄 Página {page_num}/{page_fin} — {url}")

        soup = fetch_page(url) if page_num > 1 else primera_soup
        if not soup:
            log.warning(f"Saltando página {page_num}")
            continue

        productos_pagina = parsear_pagina_cambios(soup)
        log.info(f"   → {len(productos_pagina)} productos encontrados")

        for item in productos_pagina:
            procesados += 1
            nombre  = item["nombre"]
            precio  = item["precio"]
            tienda_raw = item["tienda"]

            # Resolver tienda
            tienda_nombre = TIENDA_MAP.get(tienda_raw)
            if not tienda_nombre:
                # Intento búsqueda parcial
                for k, v in TIENDA_MAP.items():
                    if k in tienda_raw or tienda_raw in k:
                        tienda_nombre = v
                        break

            if not tienda_nombre:
                log.debug(f"Tienda desconocida: '{tienda_raw}' para '{nombre}'")
                sin_tienda += 1
                continue

            tienda_id = tiendas_map.get(tienda_nombre)
            if not tienda_id:
                log.debug(f"Tienda '{tienda_nombre}' no encontrada en BD")
                sin_tienda += 1
                continue

            # Fuzzy match contra catálogo de productos
            producto_match, score = buscar_producto_fuzzy(nombre, productos_db)

            if producto_match:
                producto_id = producto_match["id"]
                log.debug(f"Match ({score}%): '{nombre}' → '{producto_match['nombre']}'")
                actualizados += 1
            else:
                # Producto nuevo → insertar en catálogo
                log.info(f"Nuevo producto (score={score}%): '{nombre}' en {tienda_nombre}")
                producto_id = insertar_producto(sb, nombre, tienda_id)
                if not producto_id:
                    continue
                # Añadir a la lista en memoria para matches futuros en esta ejecución
                productos_db.append({
                    "id":            producto_id,
                    "nombre":        nombre,
                    "marca":         None,
                    "tienda_origen": tienda_id,
                })
                nuevos_prod += 1
                insertados += 1

            # Insertar/actualizar precio
            upsert_precio(sb, producto_id, tienda_id, precio, CIUDAD_SCRAPER)

            time.sleep(SLEEP_ENTRE_PRODUCTOS)

        time.sleep(SLEEP_ENTRE_PAGINAS)

        # Log de progreso cada 10 páginas
        if page_num % 10 == 0:
            log.info(
                f"📊 Progreso — procesados: {procesados} | "
                f"actualizados: {actualizados} | nuevos: {nuevos_prod} | "
                f"sin tienda: {sin_tienda}"
            )

    # Resumen final
    log.info("=" * 60)
    log.info(f"✅ Scraper finalizado")
    log.info(f"   Productos procesados : {procesados}")
    log.info(f"   Precios actualizados : {actualizados}")
    log.info(f"   Productos nuevos     : {nuevos_prod}")
    log.info(f"   Sin tienda conocida  : {sin_tienda}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper preciosdelsuper.es → Supabase")
    parser.add_argument(
        "--pages", nargs=2, type=int, metavar=("INICIO", "FIN"),
        help="Rango de páginas a procesar (ej: --pages 1 10)"
    )
    parser.add_argument(
        "--solo-cambios", action="store_true",
        help="Solo procesa productos con cambio de precio (no implementado aún)"
    )
    args = parser.parse_args()

    inicio = args.pages[0] if args.pages else 1
    fin    = args.pages[1] if args.pages else None

    main(page_inicio=inicio, page_fin=fin, solo_cambios=args.solo_cambios)
