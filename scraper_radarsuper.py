"""
scraper_radarsuper.py
=====================
Scraper diario de radarsuper.com → Supabase (gastos_app)

MEJORAS DE ROBUSTEZ v3:
  - Actualizado para nueva estructura de RadarSuper (mayo 2026)
  - Múltiples estrategias de extracción de categorías adaptadas al nuevo diseño
  - Soporte para URLs /supermercado/{cadena}
  - Extracción mejorada de productos con nuevos selectores
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
    "dia":       "Dia",
    "alcampo":   "Alcampo",
    "lidl":      "Lidl",
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
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15"),
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
    r"(\d{1,4})\s*€(?!\s*/)",         # 2€ (entero, sin unidad — último recurso)
]

_PATRONES_PRECIO_KG = [
    r"(\d{1,4}[.,]\d+)\s*€\s*/\s*(kg|Kg|KG|g|gr|l|L|lt|ud|und|unid)",
    r"(\d{1,4}[.,]\d+)\s*€/(kg|l|ud)",
    r"([\d.,]+)\s*€\s+(?:el\s+)?(kg|kilo|litro?|ud)",
    r"(\d{1,4}[.,]\d+)\s*(?:€/kg|€/l|€/ud)",  # Formato: 2.99€/kg
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
        "[data-testid='product-name']",
    ]

    for selector in selectores:
        el = elemento.select_one(selector)
        if el:
            texto = el.get_text(strip=True)
            if texto and len(texto) >= 4:
                return _limpiar_nombre(texto)

    # Buscar cualquier texto que no sea precio
    for texto_elemento in elemento.find_all(text=True, recursive=True):
        texto = texto_elemento.strip()
        if texto and len(texto) >= 4 and not re.search(r"\d+[.,]\d+\s*€", texto):
            if not re.match(r"^\d+$", texto):  # No es solo un número
                return _limpiar_nombre(texto[:150])

    return None


def _limpiar_nombre(nombre: str) -> str:
    """Normaliza el nombre eliminando espacios dobles, caracteres raros, etc."""
    nombre = re.sub(r"\s+", " ", nombre)
    # Eliminar sufijos numéricos de conteo ("Cereales42" → "Cereales")
    nombre = re.sub(r"\d+$", "", nombre)
    # Eliminar "Nuevo" o etiquetas similares al inicio
    nombre = re.sub(r"^(NUEVO|NEW|OFERTA)\s*[-:]\s*", "", nombre, flags=re.IGNORECASE)
    # Eliminar caracteres no válidos manteniendo acentos y puntuación básica
    nombre = re.sub(
        r"[^\w\sáéíóúàèìòùäëïöüñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑÇ.,\-%()/]", "", nombre
    )
    return nombre.strip()


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE CATEGORÍAS — múltiples estrategias (actualizado para nueva estructura)
# ─────────────────────────────────────────────────────────────────────────────
def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """
    Extrae todas las categorías del catálogo de una cadena.
    Versión actualizada para la nueva estructura de RadarSuper (mayo 2026).
    Prueba múltiples estrategias en orden de especificidad.
    """
    
    # ESTRATEGIA 1: URL con /supermercado/ (nueva estructura)
    urls_entrada = [
        f"{BASE_URL}/supermercado/{cadena_slug}",
        f"{BASE_URL}/supermercado/{cadena_slug}/categorias",
        f"{BASE_URL}/{cadena_slug}",
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

    # ESTRATEGIA 2: Buscar enlaces de categoría con patrones actualizados
    categorias = []
    
    # Patrones de URL para categorías en la nueva estructura
    # Ejemplos observados:
    # - /supermercado/mercadona/c/aceite-12
    # - /c/aceite-12
    # - /categoria/aceite
    patrones_url_categoria = [
        r"/supermercado/[\w-]+/c/([\w-]+)(?:-\d+)?",      # /supermercado/mercadona/c/aceite-12
        r"/c/([\w-]+)(?:-\d+)?",                          # /c/aceite-12
        r"/categoria/([\w-]+)",                           # /categoria/aceite
        r"/supermercado/[\w-]+/([\w-]+)(?:/|$)",          # /supermercado/mercadona/aceite
    ]
    
    vistos = set()
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        texto = a.get_text(strip=True)
        
        # Filtros: excluir productos, login, etc.
        if any(x in href.lower() for x in ["/p/", "/producto", "login", "carrito", "account", "wishlist"]):
            continue
        
        if not texto or len(texto) < 3 or len(texto) > 80:
            continue
        
        # Intentar hacer match con algún patrón de categoría
        slug_cat = None
        for patron in patrones_url_categoria:
            match = re.search(patron, href)
            if match:
                slug_cat = match.group(1)
                break
        
        if not slug_cat:
            # Si no hay match con patrones, ver si parece una categoría por el texto
            # y porque está dentro de un menú de navegación
            padre = a.find_parent(["nav", "ul", "div"])
            if padre and any(cls in str(padre.get("class", [])).lower() for cls in ["menu", "nav", "categ"]):
                slug_cat = texto.lower().replace(" ", "-").replace("_", "-")
        
        if slug_cat and slug_cat not in vistos:
            vistos.add(slug_cat)
            texto_limpio = re.sub(r"\s*\(\d+\)\s*$", "", texto).strip()
            url_categoria = href if href.startswith("http") else BASE_URL + href
            
            categorias.append({
                "slug":    slug_cat,
                "nombre":  texto_limpio,
                "url":     url_categoria,
                "cat_app": _resolver_categoria(slug_cat),
            })
    
    # Si no encontramos categorías con el método anterior, intentar extracción genérica
    if not categorias:
        log.warning(f"⚠️ No se detectaron categorías con patrones específicos para '{cadena_slug}'. Intentando extracción genérica...")
        categorias = _extraer_categorias_generico(soup, cadena_slug)
    
    if not categorias:
        log.error("❌ No se pudieron extraer categorías de ninguna forma. Guardando HTML para depuración...")
        if diagnostico:
            _guardar_diagnostico(url_usada, str(soup))
        return []
    
    log.info(f"  → {len(categorias)} categorías encontradas")
    return categorias


def _extraer_categorias_generico(soup: BeautifulSoup, cadena_slug: str) -> list[dict]:
    """
    Extracción de último recurso: busca cualquier link que parezca una categoría
    basándose en el texto y contexto.
    """
    categorias = []
    vistos = set()
    
    # Buscar en contenedores típicos de navegación
    contenedores = soup.find_all(["nav", "aside", "div", "ul"], 
                                 class_=re.compile(r"(menu|nav|categor|sidebar|filter|depart)", re.IGNORECASE))
    
    if not contenedores:
        # Si no hay contenedores específicos, buscar cualquier lista
        contenedores = soup.find_all(["ul", "div"])
    
    for contenedor in contenedores:
        for a in contenedor.find_all("a", href=True):
            href = a["href"]
            texto = a.get_text(strip=True)
            
            # Condiciones para ser categoría
            if not texto or len(texto) < 3 or len(texto) > 60:
                continue
            
            if any(x in href.lower() for x in ["/p/", "/producto", "login", "carrito", "cuenta"]):
                continue
            
            # Evitar enlaces de paginación
            if texto.isdigit() and len(texto) <= 3:
                continue
            
            texto_limpio = re.sub(r"\s*\(\d+\)\s*$", "", texto).strip()
            slug_cat = texto_limpio.lower().replace(" ", "-").replace("_", "-")
            
            if texto_limpio and slug_cat not in vistos:
                vistos.add(slug_cat)
                url_categoria = href if href.startswith("http") else BASE_URL + href
                
                categorias.append({
                    "slug":    slug_cat,
                    "nombre":  texto_limpio,
                    "url":     url_categoria,
                    "cat_app": _resolver_categoria(slug_cat),
                })
    
    # Limitar a categorías que parecen legítimas (no demasiadas)
    categorias = [c for c in categorias if not any(x in c["nombre"].lower() 
                  for x in ["terminos", "condiciones", "privacidad", "contacto", "ayuda"])]
    
    return categorias[:50]  # Máximo 50 categorías por cadena


def _resolver_categoria(slug: str) -> str:
    """
    Mapea un slug de categoría a la categoría de gastos_app.
    Primero coincidencia exacta, luego parcial, luego 'general'.
    Registra categorías desconocidas para facilitar actualizaciones del mapa.
    """
    # Exacta
    if slug in CATEGORIA_MAP:
        return CATEGORIA_MAP[slug]
    
    # Eliminar números y prefijos comunes
    slug_limpio = re.sub(r"-\d+$", "", slug)
    if slug_limpio in CATEGORIA_MAP:
        return CATEGORIA_MAP[slug_limpio]

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
    r"/p/([\w-]+)",                      # /p/leche-entera (simplificado)
    r"/producto/([\w-]+)",              # /producto/leche-entera
    r"/[^/]+/p/([\w-]+)",               # /mercadona/p/leche-entera
    r"/[^/]+/producto/([\w-]+)",        # /mercadona/producto/leche-entera
    r"/supermercado/[^/]+/p/([\w-]+)",   # /supermercado/mercadona/p/leche-entera
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
    
    # También buscar contenedores de producto directamente
    contenedores_producto = soup.find_all(["div", "article"], 
                                          class_=re.compile(r"(product|item|card)", re.IGNORECASE))
    
    for contenedor in contenedores_producto:
        nombre = extraer_nombre(contenedor)
        if not nombre:
            continue
        
        # Buscar precio dentro del contenedor
        texto_contenedor = contenedor.get_text(" ", strip=True)
        precio = extraer_precio(texto_contenedor)
        if precio is None:
            continue
        
        precio_kg, unidad_precio = extraer_precio_kg(texto_contenedor)
        
        # Buscar URL del producto
        enlace = contenedor.find("a", href=True)
        url_producto = BASE_URL + enlace["href"] if enlace and enlace["href"].startswith("/") else None
        if not url_producto and enlace:
            url_producto = enlace["href"]
        
        if nombre and nombre not in vistos:
            vistos.add(nombre)
            productos.append({
                "nombre":        nombre,
                "precio":        precio,
                "precio_kg":     precio_kg,
                "unidad_precio": unidad_precio,
                "url":           url_producto or "",
            })
    
    # Si no encontramos productos con contenedores, buscar por enlaces
    if not productos:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not any(re.search(patron, href) for patron in _PATRONES_PRODUCTO_URL):
                continue
            
            if href in vistos:
                continue
            
            nombre = extraer_nombre(a)
            if not nombre:
                continue
            
            texto = a.get_text(" ", strip=True)
            precio = extraer_precio(texto)
            if precio is None:
                continue
            
            vistos.add(href)
            precio_kg, unidad_precio = extraer_precio_kg(texto)
            url_producto = BASE_URL + href if href.startswith("/") else href
            
            productos.append({
                "nombre":        nombre,
                "precio":        precio,
                "precio_kg":     precio_kg,
                "unidad_precio": unidad_precio,
                "url":           url_producto,
            })
    
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
        _paginas_desde_resultados,   # "Mostrando 1-24 de 156 resultados"
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
        m = re.search(r"[?&]page[=:](\d+)", a["href"])
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
        ".pager a",
        "[role='navigation'] a",
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
    for attr in ("data-total-pages", "data-pages", "data-page-count", "data-total"):
        el = soup.find(attrs={attr: True})
        if el:
            try:
                return int(el[attr])
            except (ValueError, TypeError):
                continue
    return None


def _paginas_desde_resultados(soup: BeautifulSoup) -> Optional[int]:
    """Calcula páginas a partir de "Mostrando X de Y resultados" """
    texto = soup.get_text(" ")
    patrones = [
        r"Mostrando\s+\d+\s+de\s+(\d+)\s+resultados",
        r"Showing\s+\d+\s+of\s+(\d+)\s+results",
        r"(\d+)\s+resultados\s+encontrados",
        r"(\d+)\s+products",
    ]
    for p in patrones:
        m = re.search(p, texto, re.IGNORECASE)
        if m:
            try:
                total_resultados = int(m.group(1))
                # Asumimos ~24 productos por página
                paginas = (total_resultados + 23) // 24
                return max(1, paginas)
            except (ValueError, IndexError):
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
    log.info(f"     Total páginas detectadas: {total_pags}")
    
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
    log.info(f"🛒 Scraper RadarSuper v3 — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)

    sb = get_supabase()
    log.info("✅ Conectado a Supabase")

    tiendas_map  = cargar_tiendas(sb)
    productos_db = cargar_productos(sb)

    if not tiendas_map:
        log.error("❌ No se pudieron cargar las tiendas. Abortando.")
        log.info("Asegúrate de que la tabla 'tiendas' existe y contiene las cadenas:")
        log.info(f"  - {', '.join(CADENAS.values())}")
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
