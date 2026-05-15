"""
scraper_preciosdelsuper.py
==========================
Scraper diario de preciosdelsuper.es → Supabase (gastos_app)

MEJORAS DE ROBUSTEZ v2:
  - Múltiples estrategias de extracción con fallback automático
  - Reintentos con backoff exponencial (red + HTTP 429/503)
  - Detección de cambios estructurales en la página con alertas en el log
  - Selectores CSS múltiples para cada campo
  - Sesión HTTP persistente con rotación de User-Agent
  - Validación estricta de datos antes de tocar Supabase
  - Modo diagnóstico: guarda el HTML de páginas problemáticas para inspección
  - Carga paginada de productos (evita timeout en catálogos grandes)

Uso:
  python scraper_preciosdelsuper.py                  # todas las páginas
  python scraper_preciosdelsuper.py --pages 1 5      # páginas 1-5 (test)
  python scraper_preciosdelsuper.py --diagnostico    # guarda HTML problemático

Configuración (.env):
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_KEY=tu_service_role_key
  CIUDAD_SCRAPER=Nacional
"""

from __future__ import annotations

import os
import re
import sys
import time
import logging
import argparse
import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag
from thefuzz import fuzz
from dotenv import load_dotenv
from supabase import create_client, Client

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL   = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")
CIUDAD_SCRAPER = os.getenv("CIUDAD_SCRAPER", "Nacional")

BASE_URL   = "https://preciosdelsuper.es"
CAMBIOS_URL = f"{BASE_URL}/cambios"

# Fuzzy matching
FUZZY_THRESHOLD = 70

# Pausas (segundos)
SLEEP_PAGINA    = 2.0
SLEEP_PRODUCTO  = 0.3

# Reintentos de red
MAX_REINTENTOS  = 4
BACKOFF_BASE    = 2.0   # segundos; espera = BACKOFF_BASE ** intento

# Carpeta donde se guardan HTMLs de diagnóstico
DIAG_DIR = Path("diagnostico_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO DE TIENDAS
# Añade aquí cualquier variante que aparezca en los alt de las imágenes.
# La clave es el texto en MAYÚSCULAS tal como llega de la web.
# ─────────────────────────────────────────────────────────────────────────────
TIENDA_MAP: dict[str, str] = {
    "MERCADONA":        "Mercadona",
    "CARREFOUR":        "Carrefour",
    "DIA":              "Dia",
    "DÍA":              "Dia",
    "ALCAMPO":          "Alcampo",
    "EL CORTE INGLÉS":  "El Corte Inglés",
    "EL CORTE INGLES":  "El Corte Inglés",
    "CONSUM":           "Consum",
    "GADIS":            "Gadis",
    "AHORRAMAS":        "Ahorramas",
    "FROIZ":            "Froiz",
    "LIDL":             "Lidl",
    "ALDI":             "Aldi",
    "EROSKI":           "Eroski",
    "BONPREU":          "Bonpreu",
    "ESCLAT":           "Bonpreu",
    "CAPRABO":          "Caprabo",
    "CONDIS":           "Condis",
    "SIMPLY":           "Simply",
    "SUPERSOL":         "Supersol",
    "COVIRAN":          "Coviran",
    "SPAR":             "Spar",
    "BM":               "BM Supermercados",
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — sesión con reintentos y User-Agent variable
# ─────────────────────────────────────────────────────────────────────────────
# Varios User-Agents para no ser bloqueado si el servidor filtra por UA
_USER_AGENTS = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
     "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"),
    ("Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"),
]
_ua_index = 0


def _siguiente_ua() -> str:
    global _ua_index
    ua = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
    _ua_index += 1
    return ua


def _crear_sesion() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept-Language": "es-ES,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s


_sesion: requests.Session = _crear_sesion()


def fetch_page(url: str, diagnostico: bool = False) -> Optional[BeautifulSoup]:
    """
    Descarga una página con reintentos y backoff exponencial.
    Devuelve BeautifulSoup o None si falla tras todos los intentos.
    """
    global _sesion

    for intento in range(1, MAX_REINTENTOS + 1):
        _sesion.headers["User-Agent"] = _siguiente_ua()
        try:
            r = _sesion.get(url, timeout=20)

            # Rate limiting: espera y reintenta
            if r.status_code == 429:
                espera = float(r.headers.get("Retry-After", BACKOFF_BASE ** intento))
                log.warning(f"429 Rate limit en {url} — esperando {espera:.0f}s")
                time.sleep(espera)
                continue

            # Servidor caído temporalmente
            if r.status_code in (503, 502, 504):
                espera = BACKOFF_BASE ** intento
                log.warning(f"HTTP {r.status_code} en {url} (intento {intento}/{MAX_REINTENTOS}) — reintentando en {espera:.0f}s")
                time.sleep(espera)
                # Nueva sesión por si la conexión quedó en mal estado
                _sesion = _crear_sesion()
                continue

            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            # Diagnóstico: guardar HTML si la página parece vacía o con error
            if diagnostico or _parece_vacia(soup):
                _guardar_diagnostico(url, r.text)

            return soup

        except requests.exceptions.ConnectionError:
            espera = BACKOFF_BASE ** intento
            log.warning(f"Error de conexión en {url} (intento {intento}/{MAX_REINTENTOS}) — reintentando en {espera:.0f}s")
            time.sleep(espera)
            _sesion = _crear_sesion()

        except requests.exceptions.Timeout:
            espera = BACKOFF_BASE ** intento
            log.warning(f"Timeout en {url} (intento {intento}/{MAX_REINTENTOS}) — reintentando en {espera:.0f}s")
            time.sleep(espera)

        except requests.RequestException as e:
            log.error(f"Error irrecuperable en {url}: {e}")
            return None

    log.error(f"No se pudo descargar {url} tras {MAX_REINTENTOS} intentos")
    return None


def _parece_vacia(soup: BeautifulSoup) -> bool:
    """Detecta páginas que no tienen contenido útil (redirección a login, error, etc.)."""
    texto = soup.get_text(strip=True)
    if len(texto) < 200:
        return True
    # Señales de que la web cambió su estructura de login/captcha
    alertas = ["acceso restringido", "captcha", "403", "503", "error interno"]
    return any(a in texto.lower() for a in alertas)


def _guardar_diagnostico(url: str, html: str):
    """Guarda el HTML de una página problemática para inspección manual."""
    try:
        DIAG_DIR.mkdir(exist_ok=True)
        nombre = hashlib.md5(url.encode()).hexdigest()[:8]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = DIAG_DIR / f"{ts}_{nombre}.html"
        path.write_text(html, encoding="utf-8")
        log.warning(f"🔍 HTML de diagnóstico guardado en {path}")
    except Exception as e:
        log.debug(f"No se pudo guardar diagnóstico: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRECIO — múltiples estrategias
# ─────────────────────────────────────────────────────────────────────────────
# Patrones de precio ordenados de más a menos específico
_PATRONES_PRECIO = [
    r"(\d{1,4}[.,]\d{2})\s*€",       # 1,95€ · 12.50€
    r"€\s*(\d{1,4}[.,]\d{2})",       # €1,95
    r"(\d{1,4}[.,]\d{2})\s*eur",     # 1.95 eur
    r"precio[:\s]+(\d{1,4}[.,]\d+)", # precio: 1,95
    r"(\d{1,4})\s*€",                # 2€ (sin decimales, último recurso)
]


def extraer_precio(texto: str) -> Optional[float]:
    """Intenta extraer un precio del texto usando múltiples patrones."""
    texto_lower = texto.lower()
    for patron in _PATRONES_PRECIO:
        m = re.search(patron, texto_lower)
        if m:
            try:
                valor = float(m.group(1).replace(",", "."))
                # Sanity check: precios de supermercado entre 0.05€ y 999€
                if 0.05 <= valor <= 999.0:
                    return valor
            except ValueError:
                continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE NOMBRE — múltiples selectores CSS
# ─────────────────────────────────────────────────────────────────────────────
# Selectores ordenados de mayor a menor especificidad esperada.
# Si la web cambia sus clases CSS, los siguientes actúan de fallback.
_SELECTORES_NOMBRE = [
    ".product-name",
    ".product-title",
    "[class*='product'] h2",
    "[class*='product'] h3",
    "[class*='nombre']",
    "[class*='titulo']",
    "h2",
    "h3",
    "p:first-of-type",
]


def extraer_nombre(elemento: Tag) -> Optional[str]:
    """Prueba múltiples selectores para extraer el nombre del producto."""
    for selector in _SELECTORES_NOMBRE:
        el = elemento.select_one(selector)
        if el:
            texto = el.get_text(strip=True)
            if texto and len(texto) >= 3:
                return _limpiar_nombre(texto)

    # Último recurso: texto directo del elemento
    texto = elemento.get_text(" ", strip=True)
    # Eliminar el precio del texto para no confundirlo con el nombre
    texto_limpio = re.sub(r"\d+[.,]\d+\s*€.*", "", texto).strip()
    if len(texto_limpio) >= 3:
        return _limpiar_nombre(texto_limpio[:120])

    return None


def _limpiar_nombre(nombre: str) -> str:
    """Normaliza el nombre: elimina espacios dobles, caracteres raros, etc."""
    nombre = re.sub(r"\s+", " ", nombre)
    nombre = re.sub(r"[^\w\sáéíóúàèìòùäëïöüñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑÇ.,\-%()/]", "", nombre)
    return nombre.strip()


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE TIENDA — múltiples estrategias
# ─────────────────────────────────────────────────────────────────────────────
_SELECTORES_TIENDA = [
    "img[alt]",
    "[class*='tienda'] img",
    "[class*='supermercado'] img",
    "[class*='logo'] img",
    "img[src*='logo']",
    "img[src*='tienda']",
    "img[src*='supermercado']",
]


def extraer_tienda_raw(elemento: Tag) -> str:
    """Extrae el nombre crudo de la tienda probando múltiples estrategias."""
    # Estrategia 1: alt de la imagen
    for selector in _SELECTORES_TIENDA:
        img = elemento.select_one(selector)
        if img:
            alt = img.get("alt", "").strip()
            if alt and len(alt) >= 2:
                return alt.upper()
            # Si el alt está vacío, intenta con el src
            src = img.get("src", "")
            nombre_archivo = Path(src).stem.upper()
            if nombre_archivo:
                return nombre_archivo

    # Estrategia 2: texto de elementos con clase relacionada con tienda
    for selector in ["[class*='tienda']", "[class*='supermercado']", "[class*='cadena']"]:
        el = elemento.select_one(selector)
        if el:
            texto = el.get_text(strip=True).upper()
            if texto:
                return texto

    return ""


def resolver_tienda(tienda_raw: str, tiendas_map: dict[str, str]) -> Optional[str]:
    """
    Convierte el nombre crudo de tienda al nombre normalizado.
    Primero busca coincidencia exacta, luego parcial.
    """
    if not tienda_raw:
        return None

    # Coincidencia exacta
    if tienda_raw in TIENDA_MAP:
        nombre_norm = TIENDA_MAP[tienda_raw]
        return tiendas_map.get(nombre_norm)

    # Coincidencia parcial (por si llega "SUPERMERCADOS MERCADONA")
    for clave, nombre_norm in TIENDA_MAP.items():
        if clave in tienda_raw or tienda_raw in clave:
            return tiendas_map.get(nombre_norm)

    # Fuzzy matching como último recurso para nombres de tienda
    mejor_score = 0
    mejor_id = None
    for nombre_norm, tid in tiendas_map.items():
        score = fuzz.ratio(tienda_raw.lower(), nombre_norm.lower())
        if score > mejor_score and score >= 80:
            mejor_score = score
            mejor_id = tid

    if mejor_id:
        log.debug(f"Tienda resuelta por fuzzy (score={mejor_score}): '{tienda_raw}'")

    return mejor_id


# ─────────────────────────────────────────────────────────────────────────────
# PAGINACIÓN — múltiples estrategias de detección
# ─────────────────────────────────────────────────────────────────────────────
def get_total_paginas(soup: BeautifulSoup) -> int:
    """
    Detecta el número total de páginas probando múltiples patrones de paginador.
    Si no puede determinarlo, devuelve 1 (así el scraper siempre avanza).
    """
    estrategias = [
        # Estrategia 1: enlace "Última" o "Last" con page= en el href
        lambda s: _paginas_desde_enlace_ultima(s),
        # Estrategia 2: todos los números de página visibles en la paginación
        lambda s: _paginas_desde_numeros(s),
        # Estrategia 3: metadato JSON-LD o data attributes
        lambda s: _paginas_desde_meta(s),
    ]

    for estrategia in estrategias:
        resultado = estrategia(soup)
        if resultado and resultado > 0:
            log.info(f"📄 Total de páginas detectado: {resultado}")
            return resultado

    log.warning("⚠️  No se pudo detectar el número de páginas — procesando solo página 1")
    return 1


def _paginas_desde_enlace_ultima(soup: BeautifulSoup) -> Optional[int]:
    """Busca el enlace con el número más alto en el paginador."""
    max_page = 0
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            num = int(m.group(1))
            max_page = max(max_page, num)
    return max_page if max_page > 0 else None


def _paginas_desde_numeros(soup: BeautifulSoup) -> Optional[int]:
    """Busca el número más alto en elementos de paginación."""
    selectores_paginacion = [
        "nav[aria-label*='paginaci'] a",
        ".pagination a",
        ".paginacion a",
        "[class*='pag'] a",
        "[class*='page'] a",
    ]
    for selector in selectores_paginacion:
        elementos = soup.select(selector)
        numeros = []
        for el in elementos:
            texto = el.get_text(strip=True)
            if texto.isdigit():
                numeros.append(int(texto))
        if numeros:
            return max(numeros)
    return None


def _paginas_desde_meta(soup: BeautifulSoup) -> Optional[int]:
    """Busca metadatos de paginación en data attributes o JSON-LD."""
    # Busca data-total-pages o similar
    el = soup.find(attrs={"data-total-pages": True})
    if el:
        try:
            return int(el["data-total-pages"])
        except (ValueError, TypeError):
            pass

    el = soup.find(attrs={"data-pages": True})
    if el:
        try:
            return int(el["data-pages"])
        except (ValueError, TypeError):
            pass

    return None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS DE UNA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────
# Selectores para los contenedores de producto, ordenados por especificidad
_SELECTORES_CARD = [
    "a[href*='/producto/']",
    "a[href*='/product/']",
    "[class*='product-card']",
    "[class*='producto-card']",
    "[class*='item-producto']",
    "[class*='product-item']",
    "article",
    "[class*='card']",
]


def parsear_pagina(soup: BeautifulSoup, diagnostico: bool = False) -> list[dict]:
    """
    Extrae los productos de una página.
    Prueba múltiples selectores y estrategias de extracción.
    """
    productos: list[dict] = []
    vistos: set[str] = set()
    cards_encontradas = 0

    for selector in _SELECTORES_CARD:
        elementos = soup.select(selector)
        if not elementos:
            continue

        log.debug(f"Usando selector '{selector}' → {len(elementos)} elementos")
        cards_encontradas = len(elementos)

        for elemento in elementos:
            # Deduplicar por URL del producto
            href = elemento.get("href", "")
            if not href and elemento.name != "a":
                enlace = elemento.select_one("a[href]")
                href = enlace.get("href", "") if enlace else ""

            if href and href in vistos:
                continue
            if href:
                vistos.add(href)

            nombre = extraer_nombre(elemento)
            if not nombre:
                continue

            precio = extraer_precio(elemento.get_text(" ", strip=True))
            if precio is None:
                continue

            tienda_raw = extraer_tienda_raw(elemento)

            url_producto = ""
            if href:
                url_producto = (BASE_URL + href) if href.startswith("/") else href

            productos.append({
                "nombre":     nombre,
                "precio":     precio,
                "tienda_raw": tienda_raw,
                "url":        url_producto,
            })

        # Si encontramos productos con este selector, no seguimos probando
        if productos:
            break

    if cards_encontradas > 0 and not productos:
        # Se encontraron elementos pero no se pudieron parsear → posible cambio estructural
        log.warning(
            f"⚠️  CAMBIO ESTRUCTURAL DETECTADO: se encontraron {cards_encontradas} "
            f"elementos pero no se extrajo ningún producto. "
            f"Revisa los selectores CSS o guarda un diagnóstico con --diagnostico."
        )

    return productos


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "Faltan SUPABASE_URL o SUPABASE_KEY. "
            "Crea un archivo .env o define las variables de entorno."
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def cargar_tiendas(sb: Client) -> dict[str, str]:
    """Devuelve {nombre_tienda: uuid}"""
    try:
        res = sb.table("tiendas").select("id, nombre").execute()
        return {r["nombre"]: r["id"] for r in res.data}
    except Exception as e:
        log.error(f"Error cargando tiendas: {e}")
        return {}


def cargar_productos(sb: Client, page_size: int = 1000) -> list[dict]:
    """
    Carga todos los productos en páginas para no hacer timeout
    en catálogos grandes.
    """
    todos: list[dict] = []
    offset = 0
    while True:
        try:
            res = (
                sb.table("productos")
                .select("id, nombre, marca, tienda_origen")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = res.data
            todos.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        except Exception as e:
            log.error(f"Error cargando productos (offset={offset}): {e}")
            break

    log.info(f"📦 {len(todos)} productos cargados de Supabase")
    return todos


def buscar_producto_fuzzy(nombre: str, productos: list[dict]) -> tuple[Optional[dict], int]:
    """Busca el producto más similar. Devuelve (producto, score)."""
    mejor: Optional[dict] = None
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


def insertar_producto(sb: Client, nombre: str, tienda_id: str) -> Optional[str]:
    """Inserta un producto nuevo y devuelve su UUID."""
    for intento in range(1, 3):
        try:
            res = sb.table("productos").insert({
                "nombre":        nombre,
                "tienda_origen": tienda_id,
                "verificado":    False,
            }).execute()
            if res.data:
                return res.data[0]["id"]
        except Exception as e:
            log.error(f"Error insertando producto '{nombre}' (intento {intento}): {e}")
            time.sleep(1)
    return None


def upsert_precio(sb: Client, producto_id: str, tienda_id: str, precio: float, ciudad: str):
    """Inserta o actualiza el precio del día."""
    for intento in range(1, 3):
        try:
            sb.table("precios").upsert(
                {
                    "producto_id": producto_id,
                    "tienda_id":   tienda_id,
                    "ciudad":      ciudad,
                    "precio":      precio,
                    "fecha":       str(date.today()),
                    "fuente":      "scraper",
                },
                on_conflict="producto_id,tienda_id,ciudad,fecha",
            ).execute()
            return
        except Exception as e:
            log.error(f"Error upsert precio producto_id={producto_id} (intento {intento}): {e}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(
    page_inicio: int = 1,
    page_fin: Optional[int] = None,
    diagnostico: bool = False,
):
    inicio_ts = datetime.now()
    log.info("=" * 65)
    log.info(f"🛒 Scraper preciosdelsuper.es — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)

    # ── Conexión Supabase ─────────────────────────────────────────────────
    sb = get_supabase()
    log.info("✅ Conectado a Supabase")

    tiendas_map  = cargar_tiendas(sb)
    productos_db = cargar_productos(sb)

    if not tiendas_map:
        log.error("❌ No se pudieron cargar las tiendas. Abortando.")
        sys.exit(1)

    # ── Primera página para detectar total ───────────────────────────────
    primera_soup = fetch_page(CAMBIOS_URL, diagnostico=diagnostico)
    if not primera_soup:
        log.error("❌ No se pudo acceder a preciosdelsuper.es. Abortando.")
        sys.exit(1)

    total_paginas = get_total_paginas(primera_soup)
    if page_fin is None or page_fin > total_paginas:
        page_fin = total_paginas

    log.info(f"📄 Procesando páginas {page_inicio} → {page_fin} de {total_paginas} disponibles")
    log.info(f"🏙️  Ciudad scraper: {CIUDAD_SCRAPER}")

    # ── Contadores ────────────────────────────────────────────────────────
    stats = {
        "procesados":    0,
        "actualizados":  0,
        "nuevos_prod":   0,
        "sin_tienda":    0,
        "sin_precio":    0,
        "errores_db":    0,
        "paginas_vacias": 0,
    }

    # ── Bucle principal ───────────────────────────────────────────────────
    for page_num in range(page_inicio, page_fin + 1):
        url = f"{CAMBIOS_URL}?page={page_num}" if page_num > 1 else CAMBIOS_URL
        log.info(f"━━ Página {page_num}/{page_fin} ━━ {url}")

        soup = primera_soup if page_num == 1 else fetch_page(url, diagnostico=diagnostico)
        if not soup:
            log.warning(f"Saltando página {page_num}")
            stats["paginas_vacias"] += 1
            continue

        items = parsear_pagina(soup, diagnostico=diagnostico)
        log.info(f"   → {len(items)} productos extraídos")

        if not items:
            stats["paginas_vacias"] += 1
            # Tres páginas vacías consecutivas → posible problema estructural
            if stats["paginas_vacias"] >= 3 and page_num <= page_inicio + 5:
                log.error(
                    "❌ Las primeras páginas están vacías. "
                    "Posible cambio en la estructura de la web. "
                    "Ejecuta con --diagnostico para guardar el HTML."
                )
                break

        for item in items:
            stats["procesados"] += 1

            # Validar datos mínimos
            if not item["nombre"] or item["precio"] is None:
                stats["sin_precio"] += 1
                continue

            # Resolver tienda
            tienda_id = resolver_tienda(item["tienda_raw"], tiendas_map)
            if not tienda_id:
                if item["tienda_raw"]:
                    log.debug(f"Tienda no reconocida: '{item['tienda_raw']}' para '{item['nombre']}'")
                stats["sin_tienda"] += 1
                continue

            # Fuzzy match
            match, score = buscar_producto_fuzzy(item["nombre"], productos_db)

            if match:
                producto_id = match["id"]
                log.debug(f"Match ({score}%): '{item['nombre']}' → '{match['nombre']}'")
                stats["actualizados"] += 1
            else:
                log.info(f"Nuevo producto (score_max={score}%): '{item['nombre']}' [{item['tienda_raw']}]")
                producto_id = insertar_producto(sb, item["nombre"], tienda_id)
                if not producto_id:
                    stats["errores_db"] += 1
                    continue
                # Añadir a memoria para matches en esta misma ejecución
                productos_db.append({
                    "id":            producto_id,
                    "nombre":        item["nombre"],
                    "marca":         None,
                    "tienda_origen": tienda_id,
                })
                stats["nuevos_prod"] += 1

            upsert_precio(sb, producto_id, tienda_id, item["precio"], CIUDAD_SCRAPER)
            time.sleep(SLEEP_PRODUCTO)

        time.sleep(SLEEP_PAGINA)

        # Log de progreso cada 10 páginas
        if page_num % 10 == 0:
            _log_progreso(stats, page_num, page_fin)

    # ── Resumen final ─────────────────────────────────────────────────────
    duracion = datetime.now() - inicio_ts
    log.info("=" * 65)
    log.info(f"✅ Scraper finalizado en {duracion}")
    log.info(f"   Productos procesados  : {stats['procesados']}")
    log.info(f"   Precios actualizados  : {stats['actualizados']}")
    log.info(f"   Productos nuevos      : {stats['nuevos_prod']}")
    log.info(f"   Sin tienda conocida   : {stats['sin_tienda']}")
    log.info(f"   Sin precio extraíble  : {stats['sin_precio']}")
    log.info(f"   Errores Supabase      : {stats['errores_db']}")
    log.info(f"   Páginas vacías/error  : {stats['paginas_vacias']}")
    log.info("=" * 65)

    # Guardar resumen en JSON para GitHub Actions Summary
    _guardar_resumen_json(stats, duracion)


def _log_progreso(stats: dict, page_actual: int, page_fin: int):
    log.info(
        f"📊 Progreso p.{page_actual}/{page_fin} — "
        f"procesados: {stats['procesados']} | "
        f"actualizados: {stats['actualizados']} | "
        f"nuevos: {stats['nuevos_prod']} | "
        f"sin_tienda: {stats['sin_tienda']}"
    )


def _guardar_resumen_json(stats: dict, duracion):
    """Guarda un resumen en JSON para consulta rápida o integración futura."""
    try:
        resumen = {
            "fecha":    str(date.today()),
            "duracion": str(duracion),
            **stats,
        }
        Path("scraper_resumen.json").write_text(
            json.dumps(resumen, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper preciosdelsuper.es → Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--pages", nargs=2, type=int, metavar=("INICIO", "FIN"),
        help="Rango de páginas (ej: --pages 1 10)",
    )
    parser.add_argument(
        "--diagnostico", action="store_true",
        help="Guarda el HTML de páginas problemáticas para inspección manual",
    )
    args = parser.parse_args()

    inicio = args.pages[0] if args.pages else 1
    fin    = args.pages[1] if args.pages else None

    main(page_inicio=inicio, page_fin=fin, diagnostico=args.diagnostico)
