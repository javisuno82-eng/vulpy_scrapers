"""
scraper_radarsuper.py
=====================
Scraper diario de radarsuper.com → Supabase (gastos_app)

VERSIÓN v4 - CORREGIDA (mayo 2026)
  - Corregida la extracción de categorías usando la estructura real:
    https://radarsuper.com/{cadena}/c/{categoria}-{id}
  - Extracción robusta de productos desde páginas de categoría
  - Reintentos con backoff exponencial (red + HTTP 429/503)
  - Rotación de User-Agent para evitar bloqueos
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
# Clave = slug de URL, valor = nombre exacto en tabla `tiendas` de Supabase.
# ─────────────────────────────────────────────────────────────────────────────
CADENAS: dict[str, str] = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
    "dia":       "Dia",
    "alcampo":   "Alcampo",
    "lidl":      "Lidl",
}

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO DE CATEGORÍAS
# Clave = slug de categoría en RadarSuper, valor = categoría en gastos_app.
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIA_MAP: dict[str, str] = {
    "aceite-especias-salsas":      "condimentos",
    "aceite-vinagre-sal":          "condimentos",
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
    # Nuevas categorías observadas
    "bebidas-energeticas":         "bebidas",
    "cafe":                        "café",
    "dulces-y-chocolates":         "dulces",
    "higiene-bucal":               "higiene",
    "higiene-facial":              "higiene",
    "leche-y-derivados":           "lácteos",
    "pan-y-bollería":              "pan",
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
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"),
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
        "Cache-Control": "no-cache",
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
            log.debug(f"Fetching {url} (intento {intento})")
            r = _sesion.get(url, timeout=30)

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

            if r.status_code == 403:
                log.warning(f"HTTP 403 en {url} — puede que hayamos sido bloqueados")
                if diagnostico:
                    _guardar_diagnostico(url, r.text)
                return None

            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            if diagnostico and _parece_vacia(soup):
                _guardar_diagnostico(url, r.text)

            return soup

        except requests.exceptions.ConnectionError as e:
            espera = BACKOFF_BASE ** intento
            log.warning(f"Error de conexión en {url} (intento {intento}/{MAX_REINTENTOS}): {e}")
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
    alertas = ["acceso restringido", "captcha", "403", "503", "error interno", "access denied", "cloudflare"]
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
    r"price[:\s]+(\d{1,4}[.,]\d+)",   # price: 1.14
    r"(\d{1,4})\s*€(?!\s*/)",         # 2€ (entero)
]

_PATRONES_PRECIO_KG = [
    r"(\d{1,4}[.,]\d+)\s*€\s*/\s*(kg|Kg|KG|g|gr|l|L|lt|ud|und|unid)",
    r"(\d{1,4}[.,]\d+)\s*€/(kg|l|ud)",
    r"([\d.,]+)\s*€\s+(?:el\s+)?(kg|kilo|litro?|ud)",
    r"(\d{1,4}[.,]\d+)\s*(?:€/kg|€/l|€/ud)",
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
                unidad = m.group(2).lower() if len(m.groups()) > 1 else "kg"
                unidad = {"kilo": "kg", "litro": "L", "litros": "L",
                          "lt": "L", "l": "L", "gr": "g", "und": "ud",
                          "unid": "ud"}.get(unidad, unidad)
                if 0.01 <= valor <= 9999.0:
                    return valor, unidad
            except ValueError:
                continue
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE NOMBRE
# ─────────────────────────────────────────────────────────────────────────────
def extraer_nombre(elemento: Tag) -> Optional[str]:
    """Extrae el nombre del producto probando múltiples selectores."""
    selectores = [
        ".product-name",
        ".product-title",
        ".producto-nombre",
        ".producto-titulo",
        "[class*='nombre']",
        "[class*='titulo']",
        "[class*='name']",
        "[class*='title']",
        "h2", "h3", "h4", "h5",
        "p:first-of-type",
        "span:first-of-type",
        ".card-title",
    ]

    for selector in selectores:
        el = elemento.select_one(selector)
        if el:
            texto = el.get_text(strip=True)
            if texto and len(texto) >= 4:
                return _limpiar_nombre(texto)

    # Último recurso: primer texto significativo que no sea precio
    for texto in elemento.stripped_strings:
        if len(texto) >= 4 and not re.search(r"\d+[.,]\d+\s*€", texto):
            if not re.match(r"^\d+$", texto):
                return _limpiar_nombre(texto[:150])

    return None


def _limpiar_nombre(nombre: str) -> str:
    """Normaliza el nombre eliminando espacios dobles y caracteres raros."""
    nombre = re.sub(r"\s+", " ", nombre)
    nombre = re.sub(r"\d+$", "", nombre)  # Eliminar números al final
    nombre = re.sub(r"^(NUEVO|NEW|OFERTA)\s*[-:]\s*", "", nombre, flags=re.IGNORECASE)
    nombre = re.sub(r"[^\w\sáéíóúàèìòùäëïöüñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑÇ.,\-%()/]", "", nombre)
    return nombre.strip()


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE CATEGORÍAS - VERSIÓN CORREGIDA
# ─────────────────────────────────────────────────────────────────────────────
def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """
    Extrae las categorías de una cadena desde la página principal del supermercado.
    Estructura real: https://radarsuper.com/mercadona
    Los enlaces a categorías tienen el formato: /mercadona/c/nombre-categoria-123
    """
    url_principal = f"{BASE_URL}/{cadena_slug}"
    
    soup = fetch(url_principal, diagnostico=diagnostico)
    if not soup:
        log.error(f"No se pudo acceder al catálogo de {cadena_slug}")
        return []
    
    categorias = []
    vistos = set()
    
    # Buscar enlaces que coincidan con el patrón: /{cadena}/c/{categoria}-{id}
    patron_categoria = re.compile(rf"/{cadena_slug}/c/([\w-]+)-\d+")
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        match = patron_categoria.search(href)
        
        if not match:
            continue
        
        slug_cat = match.group(1)
        if slug_cat in vistos:
            continue
        
        texto = a.get_text(strip=True)
        if not texto or len(texto) < 3:
            continue
        
        # Limpiar texto: eliminar contador de productos " (86)"
        texto_limpio = re.sub(r"\s*\(\d+\)\s*$", "", texto).strip()
        vistos.add(slug_cat)
        
        url_categoria = BASE_URL + href if href.startswith("/") else href
        
        categorias.append({
            "slug":    slug_cat,
            "nombre":  texto_limpio,
            "url":     url_categoria,
            "cat_app": _resolver_categoria(slug_cat),
        })
    
    if not categorias:
        log.error(f"No se encontraron categorías para {cadena_slug}. Usa --diagnostico para depurar.")
        if diagnostico:
            _guardar_diagnostico(url_principal, str(soup))
        return []
    
    log.info(f"  → {len(categorias)} categorías encontradas")
    return categorias


def _resolver_categoria(slug: str) -> str:
    """
    Mapea un slug de categoría a la categoría de gastos_app.
    """
    # Limpiar el slug (eliminar posibles sufijos numéricos)
    slug_limpio = re.sub(r"-\d+$", "", slug)
    
    # Coincidencia exacta
    if slug_limpio in CATEGORIA_MAP:
        return CATEGORIA_MAP[slug_limpio]
    
    # Coincidencia parcial
    for clave, cat in CATEGORIA_MAP.items():
        if clave in slug_limpio or slug_limpio in clave:
            return cat
    
    log.info(f"📋 Categoría no mapeada: '{slug}' → 'general'")
    return "general"


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS - VERSIÓN CORREGIDA
# ─────────────────────────────────────────────────────────────────────────────
def parsear_productos_pagina(
    soup: BeautifulSoup, cadena_slug: str, diagnostico: bool = False
) -> list[dict]:
    """
    Extrae productos de una página de categoría.
    Busca contenedores de producto y extrae nombre, precio y URL.
    """
    productos = []
    vistos = set()
    
    # Buscar enlaces de producto (formato típico: /p/nombre-producto-123)
    patron_producto = re.compile(r"/p/[\w-]+(?:-\d+)?")
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not patron_producto.search(href):
            continue
        
        # Evitar duplicados
        if href in vistos:
            continue
        vistos.add(href)
        
        # Extraer nombre
        nombre = extraer_nombre(a)
        if not nombre:
            # Intentar con el texto del enlace
            texto = a.get_text(" ", strip=True)
            nombre = _limpiar_nombre(texto.split("€")[0].strip())
            if not nombre or len(nombre) < 4:
                continue
        
        # Extraer precio del texto circundante
        texto_completo = a.get_text(" ", strip=True)
        # También buscar en el elemento padre si es necesario
        padre = a.find_parent(["div", "article", "li"])
        if padre:
            texto_completo = padre.get_text(" ", strip=True)
        
        precio = extraer_precio(texto_completo)
        if precio is None:
            continue
        
        precio_kg, unidad_precio = extraer_precio_kg(texto_completo)
        url_producto = BASE_URL + href if href.startswith("/") else href
        
        productos.append({
            "nombre":        nombre,
            "precio":        precio,
            "precio_kg":     precio_kg,
            "unidad_precio": unidad_precio,
            "url":           url_producto,
        })
    
    # Si no encontramos productos con el patrón /p/, buscar contenedores alternativos
    if not productos:
        contenedores = soup.find_all(["div", "article"], class_=re.compile(r"(product|item|card|producto)", re.I))
        
        for contenedor in contenedores:
            nombre = extraer_nombre(contenedor)
            if not nombre:
                continue
            
            texto_completo = contenedor.get_text(" ", strip=True)
            precio = extraer_precio(texto_completo)
            if precio is None:
                continue
            
            precio_kg, unidad_precio = extraer_precio_kg(texto_completo)
            
            enlace = contenedor.find("a", href=True)
            url_producto = BASE_URL + enlace["href"] if enlace and enlace["href"].startswith("/") else ""
            
            if nombre not in vistos:
                vistos.add(nombre)
                productos.append({
                    "nombre":        nombre,
                    "precio":        precio,
                    "precio_kg":     precio_kg,
                    "unidad_precio": unidad_precio,
                    "url":           url_producto,
                })
    
    if not productos and diagnostico:
        _guardar_diagnostico("productos_no_encontrados", str(soup))
        log.warning("⚠️ No se encontraron productos en esta página")
    
    return productos


# ─────────────────────────────────────────────────────────────────────────────
# PAGINACIÓN
# ─────────────────────────────────────────────────────────────────────────────
def get_total_paginas(soup: BeautifulSoup) -> int:
    """
    Detecta el número total de páginas.
    """
    # Buscar enlaces de paginación
    patrones_pagina = [
        r"[?&]page=(\d+)",
        r"/page/(\d+)",
    ]
    
    max_page = 0
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for patron in patrones_pagina:
            m = re.search(patron, href)
            if m:
                try:
                    num = int(m.group(1))
                    max_page = max(max_page, num)
                except ValueError:
                    continue
    
    if max_page > 0:
        return max_page
    
    # Si no encuentra paginación, asumir 1 página
    return 1


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
    todos = []
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

    log.info(f"  📂 {subcat_nombre} [{cat_app}]")

    soup = fetch(url_base, diagnostico=diagnostico)
    if not soup:
        stats["categorias_error"] += 1
        return

    total_pags = get_total_paginas(soup)
    log.info(f"     Total páginas: {total_pags}")
    
    if modo_test:
        total_pags = min(total_pags, 1)

    paginas_vacias_consecutivas = 0

    for page_num in range(1, total_pags + 1):
        # Construir URL de página (formatos comunes)
        if page_num > 1:
            url = f"{url_base}?page={page_num}"
            # Alternativa: /page/2
            if "?page=" not in url and "/page/" in url_base:
                url = f"{url_base.rstrip('/')}/page/{page_num}"
        else:
            url = url_base
        
        soup_pag = soup if page_num == 1 else fetch(url, diagnostico=diagnostico)
        if not soup_pag:
            stats["paginas_error"] += 1
            paginas_vacias_consecutivas += 1
            if paginas_vacias_consecutivas >= 3:
                log.warning(f"3 páginas consecutivas fallidas en '{subcat_nombre}' — saltando")
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
                log.debug(f"Match ({score}%): '{item['nombre']}'")
            else:
                log.info(f"Nuevo producto: '{item['nombre']}'")
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
    log.info(f"🛒 Scraper RadarSuper v4 — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)

    try:
        sb = get_supabase()
        log.info("✅ Conectado a Supabase")
    except ValueError as e:
        log.error(f"❌ {e}")
        sys.exit(1)

    tiendas_map = cargar_tiendas(sb)
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
            log.warning(f"⚠️ Tienda '{cadena_nombre}' no encontrada en Supabase.")
            continue

        log.info(f"\n🏪 Procesando {cadena_nombre} ({cadena_slug})...")

        categorias = extraer_categorias(cadena_slug, diagnostico=diagnostico)

        if not categorias:
            log.error(f"No se encontraron categorías para {cadena_nombre}. Saltando.")
            continue

        if modo_test:
            categorias = categorias[:3]
            log.info(f"Modo test: {len(categorias)} categorías")

        for i, categoria in enumerate(categorias, 1):
            log.info(f"\n  [{i}/{len(categorias)}] {categoria['nombre']}")
            scrape_categoria(
                sb, cadena_slug, categoria, tienda_id,
                productos_db, stats, modo_test, diagnostico
            )
            time.sleep(SLEEP_CATEGORIA)

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

    # Guardar resumen
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
        help="Guarda el HTML de páginas problemáticas",
    )
    args = parser.parse_args()

    seleccion = list(CADENAS.keys()) if args.cadena == "todas" else [args.cadena]

    main(
        cadenas_seleccionadas=seleccion,
        modo_test=args.test,
        diagnostico=args.diagnostico,
    )
