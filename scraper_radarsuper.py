"""
scraper_radarsuper.py
=====================
Scraper diario de radarsuper.com → Supabase (gastos_app)

MEJORAS DE ROBUSTEZ v2:
  - Múltiples estrategias de extracción con fallback automático
  - Reintentos con backoff exponencial (red + HTTP 429/503)
  - Rotación de User-Agent para evitar bloqueos
  - Detección de cambios estructurales en la página con alertas en el log
  - Múltiples patrones de extracción para categorías, productos y paginación
  - Validación estricta de datos antes de tocar Supabase
  - Carga paginada de productos (evita timeout en catálogos grandes)
  - Modo diagnóstico: guarda HTML problemático para inspección manual
  - Resumen JSON al final para integración con GitHub Actions

Uso:
  python scraper_radarsuper.py                        # todo el catálogo
  python scraper_radarsuper.py --cadena mercadona     # solo Mercadona
  python scraper_radarsuper.py --cadena carrefour     # solo Carrefour
  python scraper_radarsuper.py --test                 # 3 categorías, 1 página
  python scraper_radarsuper.py --diagnostico          # guarda HTML problemático

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
import json
import logging
import argparse
import hashlib
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

BASE_URL = "https://radarsuper.com"

FUZZY_THRESHOLD = 72

SLEEP_PAGINA    = 2.0
SLEEP_CATEGORIA = 3.0
SLEEP_PRODUCTO  = 0.1

# Reintentos de red
MAX_REINTENTOS = 4
BACKOFF_BASE   = 2.0   # espera = BACKOFF_BASE ** intento

# Carpeta para HTMLs de diagnóstico
DIAG_DIR = Path("diagnostico_radarsuper")

# ─────────────────────────────────────────────────────────────────────────────
# CADENAS DISPONIBLES
# Añade aquí nuevas cadenas si RadarSuper las incorpora en el futuro.
# Clave = slug de URL, valor = nombre exacto en tabla `tiendas` de Supabase.
# ─────────────────────────────────────────────────────────────────────────────
CADENAS: dict[str, str] = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
    # "lidl":      "Lidl",     # descomentar cuando RadarSuper los añada
    # "alcampo":   "Alcampo",
}

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO DE CATEGORÍAS
# Clave = slug de categoría en RadarSuper, valor = categoría en gastos_app.
# Si RadarSuper añade una categoría nueva no mapeada, irá a "general" y
# quedará registrada en el log para que la añadas aquí.
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIA_MAP: dict[str, str] = {
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
    # Alternativas / futuras
    "lacteos":                     "lácteos",
    "frutas-y-verduras":           "fruta",
    "carniceria":                  "carne",
    "pescaderia":                  "pescado",
    "panaderia":                   "pan",
    "higiene-personal":            "higiene",
    "drogueria":                   "limpieza",
    "bebidas":                     "bebidas",
    "snacks":                      "snacks",
    "dulces":                      "dulces",
    "cafe-e-infusiones":           "café",
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper_radarsuper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — sesión con reintentos y User-Agent rotativo
# ─────────────────────────────────────────────────────────────────────────────
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


def fetch(url: str, diagnostico: bool = False) -> Optional[BeautifulSoup]:
    """
    Descarga una página con reintentos y backoff exponencial.
    Devuelve BeautifulSoup o None si falla tras todos los intentos.
    """
    global _sesion

    for intento in range(1, MAX_REINTENTOS + 1):
        _sesion.headers["User-Agent"] = _siguiente_ua()
        try:
            r = _sesion.get(url, timeout=20)

            if r.status_code == 429:
                espera = float(r.headers.get("Retry-After", BACKOFF_BASE ** intento))
                log.warning(f"429 Rate limit en {url} — esperando {espera:.0f}s")
                time.sleep(espera)
                continue

            if r.status_code in (502, 503, 504):
                espera = BACKOFF_BASE ** intento
                log.warning(f"HTTP {r.status_code} en {url} (intento {intento}/{MAX_REINTENTOS}) — reintentando en {espera:.0f}s")
                time.sleep(espera)
                _sesion = _crear_sesion()
                continue

            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

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
    texto = soup.get_text(strip=True)
    if len(texto) < 200:
        return True
    alertas = ["acceso restringido", "captcha", "403", "503", "error interno", "access denied"]
    return any(a in texto.lower() for a in alertas)


def _guardar_diagnostico(url: str, html: str):
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
# EXTRACCIÓN DE PRECIO — múltiples patrones con validación
# ─────────────────────────────────────────────────────────────────────────────
_PATRONES_PRECIO = [
    r"(\d{1,4}[.,]\d{2})\s*€",        # 1,14€ · 12.50€
    r"€\s*(\d{1,4}[.,]\d{2})",        # €1,14
    r"(\d{1,4}[.,]\d{2})\s*eur",      # 1.14 eur
    r"precio[:\s]+(\d{1,4}[.,]\d+)",  # precio: 1,14
    r"(\d{1,4})\s*€(?!\s*/)",         # 2€ (entero, sin unidad — último recurso)
]

_PATRONES_PRECIO_KG = [
    r"(\d{1,4}[.,]\d+)\s*€\s*/\s*(kg|Kg|KG|g|gr|l|L|lt|ud|und|unid)",
    r"(\d{1,4}[.,]\d+)\s*€/(kg|l|ud)",
    r"([\d.,]+)\s*€\s+(?:el\s+)?(kg|kilo|litro?|ud)",
]


def extraer_precio(texto: str) -> Optional[float]:
    """Extrae el precio principal del texto usando múltiples patrones."""
    for patron in _PATRONES_PRECIO:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            try:
                valor = float(m.group(1).replace(",", "."))
                if 0.05 <= valor <= 999.0:
                    return valor
            except ValueError:
                continue
    return None


def extraer_precio_kg(texto: str) -> tuple[Optional[float], Optional[str]]:
    """Extrae el precio por kg/L/ud y su unidad."""
    for patron in _PATRONES_PRECIO_KG:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            try:
                valor = float(m.group(1).replace(",", "."))
                unidad = m.group(2).lower()
                # Normalizar unidades
                unidad = {"kilo": "kg", "litro": "L", "litros": "L",
                          "lt": "L", "l": "L", "gr": "g", "und": "ud",
                          "unid": "ud"}.get(unidad, unidad)
                if 0.01 <= valor <= 9999.0:
                    return valor, unidad
            except ValueError:
                continue
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE NOMBRE — múltiples estrategias
# ─────────────────────────────────────────────────────────────────────────────
def extraer_nombre(elemento: Tag) -> Optional[str]:
    """
    Extrae el nombre del producto probando múltiples estrategias.
    RadarSuper puede cambiar sus clases CSS; los fallbacks garantizan
    que siempre haya una extracción posible.
    """
    selectores = [
        ".product-name",
        ".product-title",
        "[class*='nombre']",
        "[class*='titulo']",
        "[class*='name']",
        "[class*='title']",
        "h2", "h3", "h4",
        "p:first-of-type",
        "span:first-of-type",
    ]

    for selector in selectores:
        el = elemento.select_one(selector)
        if el:
            texto = el.get_text(strip=True)
            if texto and len(texto) >= 4:
                return _limpiar_nombre(texto)

    # Último recurso: texto completo sin precio
    texto_completo = elemento.get_text(" ", strip=True)
    # Eliminar precio del texto para no confundirlo con el nombre
    texto_sin_precio = re.sub(r"\d+[.,]\d+\s*€.*", "", texto_completo).strip()
    if len(texto_sin_precio) >= 4:
        # Tomar solo la primera línea significativa
        primera_linea = next(
            (l.strip() for l in texto_sin_precio.split("\n") if len(l.strip()) >= 4),
            None
        )
        if primera_linea:
            return _limpiar_nombre(primera_linea[:150])

    return None


def _limpiar_nombre(nombre: str) -> str:
    """Normaliza el nombre eliminando espacios dobles, caracteres raros, etc."""
    nombre = re.sub(r"\s+", " ", nombre)
    # Eliminar sufijos numéricos de conteo ("Cereales42" → "Cereales")
    nombre = re.sub(r"\d+$", "", nombre)
    # Eliminar caracteres no válidos manteniendo acentos y puntuación básica
    nombre = re.sub(
        r"[^\w\sáéíóúàèìòùäëïöüñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑÇ.,\-%()/]", "", nombre
    )
    return nombre.strip()


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE CATEGORÍAS — múltiples patrones de URL
# ─────────────────────────────────────────────────────────────────────────────
# Patrones de URL de categoría ordenados de más a menos específico.
# RadarSuper usa actualmente: /fitoterapia/c/fitoterapia-213
# Es decir: /{categoria_slug}/c/{categoria_slug}-{id}
# El slug de cadena NO aparece en la URL de categoría.
_PATRONES_CATEGORIA_URL = [
    r"/([\w-]+)/c/([\w-]+)-(\d+)",     # /fitoterapia/c/fitoterapia-213 (estructura actual)
    r"/([\w-]+)/c/([\w-]+)",           # /fitoterapia/c/fitoterapia (sin ID numérico)
    r"/([\w-]+)/categoria/([\w-]+)",   # /fitoterapia/categoria/fitoterapia
    r"/([\w-]+)/productos/([\w-]+)",   # /fitoterapia/productos/lista
    r"/c/([\w-]+)-(\d+)",              # /c/fitoterapia-213 (sin prefijo)
    r"/categorias/([\w-]+)",           # /categorias/fitoterapia
]


def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """
    Extrae todas las categorías del catálogo de una cadena.
    Prueba múltiples URLs de entrada y patrones si el principal falla.
    """
    urls_entrada = [
        f"{BASE_URL}/{cadena_slug}",
        f"{BASE_URL}/{cadena_slug}/productos",
        f"{BASE_URL}/supermercado/{cadena_slug}",
        f"{BASE_URL}/cadena/{cadena_slug}",
    ]

    soup = None
    url_usada = ""
    for url in urls_entrada:
        s = fetch(url, diagnostico=diagnostico)
        if s and not _parece_vacia(s):
            soup = s
            url_usada = url
            log.info(f"  → Entrada al catálogo: {url}")
            break

    if not soup:
        log.error(f"No se pudo acceder al catálogo de {cadena_slug}")
        return []

    for patron_str in _PATRONES_CATEGORIA_URL:
        patron = re.compile(patron_str)
        categorias = _extraer_categorias_con_patron(soup, patron, cadena_slug)
        if categorias:
            log.info(f"  → {len(categorias)} categorías encontradas (patrón: {patron_str})")
            return categorias

    log.warning(
        f"⚠️  CAMBIO ESTRUCTURAL: no se detectaron categorías con los patrones conocidos "
        f"para '{cadena_slug}'. Intentando extracción genérica..."
    )
    if diagnostico:
        _guardar_diagnostico(url_usada, str(soup))

    return _extraer_categorias_generico(soup, cadena_slug)


def _extraer_categorias_con_patron(
    soup: BeautifulSoup, patron: re.Pattern, cadena_slug: str
) -> list[dict]:
    """Extrae categorías usando un patrón de URL concreto."""
    categorias: list[dict] = []
    vistos: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href in vistos:
            continue

        m = patron.search(href)
        if not m:
            continue

        grupos = [g for g in m.groups() if g and not g.isdigit()]
        if not grupos:
            continue

        # El slug de categoría es el grupo más largo que no sea el slug de cadena
        slug_cat = None
        for g in reversed(grupos):
            if g and g != cadena_slug and len(g) >= 3:
                slug_cat = g
                break
        if not slug_cat:
            slug_cat = grupos[-1]

        nombre = a.get_text(strip=True)
        nombre = re.sub(r"\d+$", "", nombre).strip()

        if not nombre or len(nombre) < 3:
            continue

        # Excluir links que no son categorías de productos
        if any(x in href for x in ["/p/", "/m/", "/blog/", "/tiendas/"]):
            continue

        vistos.add(href)

        cat_app = _resolver_categoria(slug_cat)
        url_completa = (BASE_URL + href) if href.startswith("/") else href

        categorias.append({
            "slug":    slug_cat,
            "nombre":  nombre,
            "url":     url_completa,
            "cat_app": cat_app,
        })

    return categorias


def _extraer_categorias_generico(soup: BeautifulSoup, cadena_slug: str) -> list[dict]:
    """
    Extracción de último recurso: busca cualquier link de navegación
    que no sea la home ni un producto.
    """
    categorias: list[dict] = []
    vistos: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Excluir links que claramente no son categorías
        if any(x in href for x in ["/p/", "?", "#", "mailto:", "javascript:", ".pdf"]):
            continue
        if href in vistos or href in ("/", f"/{cadena_slug}"):
            continue
        if not href.startswith(f"/{cadena_slug}/"):
            continue

        vistos.add(href)
        nombre = a.get_text(strip=True)
        nombre = re.sub(r"\d+$", "", nombre).strip()
        if not nombre or len(nombre) < 3:
            continue

        slug_cat = href.split("/")[-1]
        cat_app  = _resolver_categoria(slug_cat)

        categorias.append({
            "slug":    slug_cat,
            "nombre":  nombre,
            "url":     BASE_URL + href,
            "cat_app": cat_app,
        })

    if not categorias:
        log.error(
            "❌ No se pudieron extraer categorías de ninguna forma. "
            "La estructura de la web ha cambiado significativamente. "
            "Usa --diagnostico para guardar el HTML y revisar manualmente."
        )

    return categorias


def _resolver_categoria(slug: str) -> str:
    """
    Mapea un slug de categoría a la categoría de gastos_app.
    Primero coincidencia exacta, luego parcial, luego 'general'.
    Registra categorías desconocidas para facilitar actualizaciones del mapa.
    """
    # Exacta
    if slug in CATEGORIA_MAP:
        return CATEGORIA_MAP[slug]

    # Parcial: el slug contiene o está contenido en una clave del mapa
    for clave, cat in CATEGORIA_MAP.items():
        if clave in slug or slug in clave:
            return cat

    # No mapeada — registrar para que el desarrollador la añada
    log.info(f"📋 Categoría no mapeada (irá a 'general'): '{slug}' — añádela a CATEGORIA_MAP")
    return "general"


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS — múltiples estrategias
# ─────────────────────────────────────────────────────────────────────────────
_PATRONES_PRODUCTO_URL = [
    r"/([\w-]+)/p/([\w-]+)",      # /mercadona/p/leche-entera-asturiana
    r"/([\w-]+)/producto/([\w-]+)", # /mercadona/producto/leche-entera
    r"/productos/([\w-]+)",        # /productos/leche-entera
    r"/p/([\w-]+)",                # /p/leche-entera (sin cadena)
]


def parsear_productos_pagina(
    soup: BeautifulSoup, cadena_slug: str, diagnostico: bool = False
) -> list[dict]:
    """
    Extrae productos de una página de listado.
    Prueba múltiples patrones de URL de producto.
    """
    productos: list[dict] = []
    vistos: set[str] = set()
    total_enlaces = 0

    for patron_str in _PATRONES_PRODUCTO_URL:
        patron = re.compile(patron_str)
        encontrados = soup.find_all("a", href=patron)
        if not encontrados:
            continue

        total_enlaces = len(encontrados)
        log.debug(f"Usando patrón de producto '{patron_str}' → {total_enlaces} enlaces")

        for a in encontrados:
            href = a["href"]
            if href in vistos:
                continue
            vistos.add(href)

            texto = a.get_text(" ", strip=True)

            nombre = extraer_nombre(a)
            if not nombre or len(nombre) < 4:
                # Intentar con el texto directo
                lineas = [l.strip() for l in texto.split("\n") if l.strip() and len(l.strip()) >= 4]
                if lineas:
                    nombre = _limpiar_nombre(lineas[0][:150])
            if not nombre:
                continue

            precio = extraer_precio(texto)
            if precio is None:
                continue

            precio_kg, unidad_precio = extraer_precio_kg(texto)

            url_producto = (BASE_URL + href) if href.startswith("/") else href

            productos.append({
                "nombre":        nombre,
                "precio":        precio,
                "precio_kg":     precio_kg,
                "unidad_precio": unidad_precio,
                "url":           url_producto,
            })

        # Si encontramos productos con este patrón, no seguimos
        if productos:
            break

    # Aviso de cambio estructural
    if total_enlaces > 0 and not productos:
        log.warning(
            f"⚠️  CAMBIO ESTRUCTURAL: {total_enlaces} enlaces de producto encontrados "
            f"pero no se pudo extraer ningún dato. Revisa los selectores."
        )
        if diagnostico:
            _guardar_diagnostico("pagina_producto", str(soup))

    return productos


# ─────────────────────────────────────────────────────────────────────────────
# PAGINACIÓN — múltiples estrategias de detección
# ─────────────────────────────────────────────────────────────────────────────
def get_total_paginas(soup: BeautifulSoup) -> int:
    """
    Detecta el número total de páginas probando múltiples estrategias.
    Devuelve 1 si no puede determinarlo (el scraper siempre avanza).
    """
    estrategias = [
        _paginas_desde_texto,        # "Página X de Y"
        _paginas_desde_links,        # ?page=N en los hrefs
        _paginas_desde_paginador,    # selectores CSS de paginación
        _paginas_desde_data,         # data-total-pages / data-pages
    ]

    for estrategia in estrategias:
        resultado = estrategia(soup)
        if resultado and resultado > 0:
            return resultado

    return 1


def _paginas_desde_texto(soup: BeautifulSoup) -> Optional[int]:
    texto = soup.get_text(" ")
    patrones = [
        r"[Pp]ágina\s+\d+\s+de\s+(\d+)",
        r"[Pp]age\s+\d+\s+of\s+(\d+)",
        r"(\d+)\s+páginas",
        r"total[:\s]+(\d+)\s+p[aá]ginas",
    ]
    for p in patrones:
        m = re.search(p, texto)
        if m:
            try:
                return int(m.group(1))
            except (ValueError, IndexError):
                continue
    return None


def _paginas_desde_links(soup: BeautifulSoup) -> Optional[int]:
    max_page = 0
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            try:
                num = int(m.group(1))
                max_page = max(max_page, num)
            except ValueError:
                continue
    return max_page if max_page > 0 else None


def _paginas_desde_paginador(soup: BeautifulSoup) -> Optional[int]:
    selectores = [
        "nav[aria-label*='paginaci'] a",
        ".pagination a",
        ".paginacion a",
        "[class*='pag'] a",
        "[class*='page-item'] a",
    ]
    for selector in selectores:
        elementos = soup.select(selector)
        numeros = []
        for el in elementos:
            texto = el.get_text(strip=True)
            if texto.isdigit():
                numeros.append(int(texto))
        if numeros:
            return max(numeros)
    return None


def _paginas_desde_data(soup: BeautifulSoup) -> Optional[int]:
    for attr in ("data-total-pages", "data-pages", "data-page-count"):
        el = soup.find(attrs={attr: True})
        if el:
            try:
                return int(el[attr])
            except (ValueError, TypeError):
                continue
    return None


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
    try:
        res = sb.table("tiendas").select("id, nombre").execute()
        return {r["nombre"]: r["id"] for r in res.data}
    except Exception as e:
        log.error(f"Error cargando tiendas: {e}")
        return {}


def cargar_productos(sb: Client, page_size: int = 1000) -> list[dict]:
    """Carga paginada para no hacer timeout en catálogos grandes."""
    todos: list[dict] = []
    offset = 0
    while True:
        try:
            res = (
                sb.table("productos")
                .select("id, nombre, marca, categoria")
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


def insertar_producto(
    sb: Client, nombre: str, categoria: str, subcategoria: str, tienda_id: str
) -> Optional[str]:
    for intento in range(1, 3):
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
            log.error(f"Error insertando producto '{nombre}' (intento {intento}): {e}")
            time.sleep(1)
    return None


def upsert_precio(sb: Client, producto_id: str, tienda_id: str, precio: float):
    for intento in range(1, 3):
        try:
            sb.table("precios").upsert(
                {
                    "producto_id": producto_id,
                    "tienda_id":   tienda_id,
                    "ciudad":      CIUDAD_SCRAPER,
                    "precio":      precio,
                    "fecha":       str(date.today()),
                    "fuente":      "radarsuper",
                },
                on_conflict="producto_id,tienda_id,ciudad,fecha",
            ).execute()
            return
        except Exception as e:
            log.error(f"Error upsert precio producto_id={producto_id} (intento {intento}): {e}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# PROCESO DE CATEGORÍA
# ─────────────────────────────────────────────────────────────────────────────
def scrape_categoria(
    sb: Client,
    cadena_slug: str,
    categoria: dict,
    tienda_id: str,
    productos_db: list[dict],
    stats: dict,
    modo_test: bool = False,
    diagnostico: bool = False,
):
    """Procesa todas las páginas de una categoría."""
    url_base      = categoria["url"]
    cat_app       = categoria["cat_app"]
    subcat_nombre = categoria["nombre"]

    log.info(f"  📂 {subcat_nombre} [{cat_app}] — {url_base}")

    soup = fetch(url_base, diagnostico=diagnostico)
    if not soup:
        stats["categorias_error"] += 1
        return

    total_pags = get_total_paginas(soup)
    if modo_test:
        total_pags = min(total_pags, 1)

    paginas_vacias_consecutivas = 0

    for page_num in range(1, total_pags + 1):
        url = f"{url_base}?page={page_num}" if page_num > 1 else url_base
        soup_pag = soup if page_num == 1 else fetch(url, diagnostico=diagnostico)
        if not soup_pag:
            stats["paginas_error"] += 1
            paginas_vacias_consecutivas += 1
            if paginas_vacias_consecutivas >= 3:
                log.warning(f"3 páginas consecutivas fallidas en '{subcat_nombre}' — saltando resto")
                break
            continue

        items = parsear_productos_pagina(soup_pag, cadena_slug, diagnostico=diagnostico)
        log.info(f"     Página {page_num}/{total_pags} → {len(items)} productos")

        if not items:
            paginas_vacias_consecutivas += 1
        else:
            paginas_vacias_consecutivas = 0

        for item in items:
            stats["procesados"] += 1

            if not item["nombre"] or item["precio"] is None:
                stats["sin_precio"] += 1
                continue

            match, score = buscar_producto_fuzzy(item["nombre"], productos_db)

            if match:
                producto_id = match["id"]
                stats["actualizados"] += 1
                log.debug(f"Match ({score}%): '{item['nombre']}' → '{match['nombre']}'")
            else:
                log.info(f"Nuevo producto (score_max={score}%): '{item['nombre']}' [{subcat_nombre}]")
                producto_id = insertar_producto(
                    sb, item["nombre"], cat_app, subcat_nombre, tienda_id
                )
                if not producto_id:
                    stats["errores_db"] += 1
                    continue
                productos_db.append({
                    "id":        producto_id,
                    "nombre":    item["nombre"],
                    "marca":     None,
                    "categoria": cat_app,
                })
                stats["nuevos"] += 1

            upsert_precio(sb, producto_id, tienda_id, item["precio"])
            time.sleep(SLEEP_PRODUCTO)

        time.sleep(SLEEP_PAGINA)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(
    cadenas_seleccionadas: list[str],
    modo_test: bool = False,
    diagnostico: bool = False,
):
    inicio_ts = datetime.now()
    log.info("=" * 65)
    log.info(f"🛒 Scraper RadarSuper — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)

    sb = get_supabase()
    log.info("✅ Conectado a Supabase")

    tiendas_map  = cargar_tiendas(sb)
    productos_db = cargar_productos(sb)

    if not tiendas_map:
        log.error("❌ No se pudieron cargar las tiendas. Abortando.")
        sys.exit(1)

    stats = {
        "procesados":       0,
        "actualizados":     0,
        "nuevos":           0,
        "sin_precio":       0,
        "errores_db":       0,
        "paginas_error":    0,
        "categorias_error": 0,
    }

    for cadena_slug, cadena_nombre in CADENAS.items():
        if cadena_slug not in cadenas_seleccionadas:
            continue

        tienda_id = tiendas_map.get(cadena_nombre)
        if not tienda_id:
            log.warning(
                f"⚠️  Tienda '{cadena_nombre}' no encontrada en Supabase. "
                f"Comprueba que existe en la tabla 'tiendas'."
            )
            continue

        log.info(f"\n🏪 Procesando {cadena_nombre} ({cadena_slug})...")

        categorias = extraer_categorias(cadena_slug, diagnostico=diagnostico)

        if not categorias:
            log.error(f"No se encontraron categorías para {cadena_nombre}. Saltando.")
            continue

        if modo_test:
            categorias = categorias[:3]
            log.info(f"Modo test: procesando solo {len(categorias)} categorías")

        for i, categoria in enumerate(categorias, 1):
            log.info(f"\n  [{i}/{len(categorias)}] {categoria['nombre']}")
            scrape_categoria(
                sb, cadena_slug, categoria, tienda_id,
                productos_db, stats, modo_test, diagnostico
            )
            time.sleep(SLEEP_CATEGORIA)

    # ── Resumen final ─────────────────────────────────────────────────────
    duracion = datetime.now() - inicio_ts
    log.info("=" * 65)
    log.info(f"✅ Scraper RadarSuper finalizado en {duracion}")
    log.info(f"   Productos procesados   : {stats['procesados']}")
    log.info(f"   Precios actualizados   : {stats['actualizados']}")
    log.info(f"   Productos nuevos       : {stats['nuevos']}")
    log.info(f"   Sin precio extraíble   : {stats['sin_precio']}")
    log.info(f"   Errores Supabase       : {stats['errores_db']}")
    log.info(f"   Páginas con error      : {stats['paginas_error']}")
    log.info(f"   Categorías con error   : {stats['categorias_error']}")
    log.info("=" * 65)

    # Guardar resumen JSON para GitHub Actions / monitorización
    _guardar_resumen_json(stats, duracion)


def _guardar_resumen_json(stats: dict, duracion):
    try:
        resumen = {
            "fecha":    str(date.today()),
            "duracion": str(duracion),
            **stats,
        }
        Path("scraper_radarsuper_resumen.json").write_text(
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
        description="Scraper RadarSuper → Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cadena",
        choices=list(CADENAS.keys()) + ["todas"],
        default="todas",
        help="Cadena a procesar (default: todas)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Modo test: 3 categorías, 1 página cada una",
    )
    parser.add_argument(
        "--diagnostico",
        action="store_true",
        help="Guarda el HTML de páginas problemáticas en diagnostico_radarsuper/",
    )
    args = parser.parse_args()

    seleccion = list(CADENAS.keys()) if args.cadena == "todas" else [args.cadena]

    main(
        cadenas_seleccionadas=seleccion,
        modo_test=args.test,
        diagnostico=args.diagnostico,
    )
- name: "🔍 Test HTML RadarSuper"
  run: |
    python3 - << 'EOF'
    import requests
    r = requests.get("https://radarsuper.com/mercadona", headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    print(f"Status: {r.status_code}")
    print(f"Tamaño HTML: {len(r.text)} caracteres")
    # Buscar links de categoría
    import re
    cats = re.findall(r'/mercadona/c/[\w-]+', r.text)
    print(f"Categorías encontradas en HTML: {len(cats)}")
    print("Primeras 5:", cats[:5])
    EOF
