"""
scraper_mercadona.py
====================
Scraper exclusivo para Mercadona en radarsuper.com → Supabase (gastos_app)

VERSIÓN ESPECÍFICA PARA MERCADONA
  - Usa las URLs reales de categorías de Mercadona
  - Extrae productos de las páginas de categoría
  - Reintentos con backoff exponencial
  - Rotación de User-Agent para evitar bloqueos
  - Modo diagnóstico para depuración

Uso:
  python scraper_mercadona.py                        # todo el catálogo
  python scraper_mercadona.py --test                 # 3 categorías, 1 página
  python scraper_mercadona.py --diagnostico          # guarda HTML problemático

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
BACKOFF_BASE   = 2.0

# Carpeta para HTMLs de diagnóstico
DIAG_DIR = Path("diagnostico_mercadona")

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORÍAS DE MERCADONA (mapeo manual basado en URLs reales)
# El formato es: https://radarsuper.com/mercadona/c/{slug}-{id}
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIAS_MERCADONA = [
    {
        "slug": "bebe-24",
        "nombre": "Bebé",
        "url": "https://radarsuper.com/mercadona/c/bebe-24",
        "cat_app": "higiene"
    },
    {
        "slug": "aceite-especias-salsas-12",
        "nombre": "Aceite, especias y salsas",
        "url": "https://radarsuper.com/mercadona/c/aceite-especias-salsas-12",
        "cat_app": "condimentos"
    },
    {
        "slug": "aceite-vinagre-sal-112",
        "nombre": "Aceite, vinagre y sal",
        "url": "https://radarsuper.com/mercadona/c/aceite-vinagre-sal-112",
        "cat_app": "condimentos"
    },
    {
        "slug": "carne-3",
        "nombre": "Carne",
        "url": "https://radarsuper.com/mercadona/c/carne-3",
        "cat_app": "carne"
    },
    {
        "slug": "agua-refrescos-21",
        "nombre": "Agua y refrescos",
        "url": "https://radarsuper.com/mercadona/c/agua-refrescos-21",
        "cat_app": "bebidas"
    },
    {
        "slug": "aperitivos-10",
        "nombre": "Aperitivos",
        "url": "https://radarsuper.com/mercadona/c/aperitivos-10",
        "cat_app": "snacks"
    },
    {
        "slug": "arroz-legumbres-pasta-9",
        "nombre": "Arroz, legumbres y pasta",
        "url": "https://radarsuper.com/mercadona/c/arroz-legumbres-pasta-9",
        "cat_app": "pasta"
    },
    {
        "slug": "azucar-caramelos-chocolate-8",
        "nombre": "Azúcar, caramelos y chocolate",
        "url": "https://radarsuper.com/mercadona/c/azucar-caramelos-chocolate-8",
        "cat_app": "dulces"
    },
    {
        "slug": "bodega-20",
        "nombre": "Bodega",
        "url": "https://radarsuper.com/mercadona/c/bodega-20",
        "cat_app": "bebidas"
    },
    {
        "slug": "cacao-cafe-e-infusiones-19",
        "nombre": "Cacao, café e infusiones",
        "url": "https://radarsuper.com/mercadona/c/cacao-cafe-e-infusiones-19",
        "cat_app": "café"
    },
    {
        "slug": "cereales-galletas-7",
        "nombre": "Cereales y galletas",
        "url": "https://radarsuper.com/mercadona/c/cereales-galletas-7",
        "cat_app": "galletas"
    },
    {
        "slug": "charcuteria-quesos-6",
        "nombre": "Charcutería y quesos",
        "url": "https://radarsuper.com/mercadona/c/charcuteria-quesos-6",
        "cat_app": "charcutería"
    },
    {
        "slug": "congelados-5",
        "nombre": "Congelados",
        "url": "https://radarsuper.com/mercadona/c/congelados-5",
        "cat_app": "congelados"
    },
    {
        "slug": "conservas-caldos-cremas-4",
        "nombre": "Conservas, caldos y cremas",
        "url": "https://radarsuper.com/mercadona/c/conservas-caldos-cremas-4",
        "cat_app": "conservas"
    },
    {
        "slug": "fruta-verdura-1",
        "nombre": "Fruta y verdura",
        "url": "https://radarsuper.com/mercadona/c/fruta-verdura-1",
        "cat_app": "fruta"
    },
    {
        "slug": "huevos-leche-mantequilla-18",
        "nombre": "Huevos, leche y mantequilla",
        "url": "https://radarsuper.com/mercadona/c/huevos-leche-mantequilla-18",
        "cat_app": "lácteos"
    },
    {
        "slug": "limpieza-hogar-25",
        "nombre": "Limpieza del hogar",
        "url": "https://radarsuper.com/mercadona/c/limpieza-hogar-25",
        "cat_app": "limpieza"
    },
    {
        "slug": "marisco-pescado-2",
        "nombre": "Marisco y pescado",
        "url": "https://radarsuper.com/mercadona/c/marisco-pescado-2",
        "cat_app": "pescado"
    },
    {
        "slug": "mascotas-29",
        "nombre": "Mascotas",
        "url": "https://radarsuper.com/mercadona/c/mascotas-29",
        "cat_app": "mascotas"
    },
    {
        "slug": "panaderia-pasteleria-17",
        "nombre": "Panadería y pastelería",
        "url": "https://radarsuper.com/mercadona/c/panaderia-pasteleria-17",
        "cat_app": "pan"
    },
    {
        "slug": "pizzas-platos-preparados-11",
        "nombre": "Pizzas y platos preparados",
        "url": "https://radarsuper.com/mercadona/c/pizzas-platos-preparados-11",
        "cat_app": "preparados"
    },
    {
        "slug": "postres-yogures-16",
        "nombre": "Postres y yogures",
        "url": "https://radarsuper.com/mercadona/c/postres-yogures-16",
        "cat_app": "lácteos"
    },
    {
        "slug": "zumos-22",
        "nombre": "Zumos",
        "url": "https://radarsuper.com/mercadona/c/zumos-22",
        "cat_app": "bebidas"
    },
    {
        "slug": "cuidado-personal-15",
        "nombre": "Cuidado personal",
        "url": "https://radarsuper.com/mercadona/c/cuidado-personal-15",
        "cat_app": "higiene"
    },
    {
        "slug": "drogueria-14",
        "nombre": "Droguería",
        "url": "https://radarsuper.com/mercadona/c/drogueria-14",
        "cat_app": "limpieza"
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper_mercadona.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — sesión con reintentos y User-Agent rotativo
# ─────────────────────────────────────────────────────────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
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


_sesion = _crear_sesion()


def fetch(url: str, diagnostico: bool = False) -> Optional[BeautifulSoup]:
    """Descarga una página con reintentos y backoff exponencial."""
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
                log.warning(f"HTTP {r.status_code} en {url} — reintentando en {espera:.0f}s")
                time.sleep(espera)
                _sesion = _crear_sesion()
                continue

            if r.status_code == 403:
                log.warning(f"HTTP 403 en {url} — posible bloqueo")
                if diagnostico:
                    _guardar_diagnostico(url, r.text)
                return None

            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            if diagnostico:
                _guardar_diagnostico(url, r.text)

            return soup

        except requests.exceptions.RequestException as e:
            espera = BACKOFF_BASE ** intento
            log.warning(f"Error en {url} (intento {intento}): {e}")
            time.sleep(espera)
            _sesion = _crear_sesion()

    log.error(f"No se pudo descargar {url} tras {MAX_REINTENTOS} intentos")
    return None


def _guardar_diagnostico(url: str, html: str):
    """Guarda HTML para depuración."""
    try:
        DIAG_DIR.mkdir(exist_ok=True)
        nombre = hashlib.md5(url.encode()).hexdigest()[:8]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = DIAG_DIR / f"{ts}_{nombre}.html"
        path.write_text(html, encoding="utf-8")
        log.warning(f"🔍 HTML guardado: {path}")
    except Exception as e:
        log.debug(f"No se pudo guardar diagnóstico: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRECIO
# ─────────────────────────────────────────────────────────────────────────────
_PATRONES_PRECIO = [
    r"(\d{1,4}[.,]\d{2})\s*€",
    r"€\s*(\d{1,4}[.,]\d{2})",
    r"(\d{1,4}[.,]\d{2})\s*eur",
    r"precio[:\s]+(\d{1,4}[.,]\d+)",
    r"(\d{1,4})\s*€",
]

_PATRONES_PRECIO_KG = [
    r"(\d{1,4}[.,]\d+)\s*€\s*/\s*(kg|Kg|KG|g|l|L|ud)",
    r"(\d{1,4}[.,]\d+)\s*€/(kg|l|ud)",
    r"(\d{1,4}[.,]\d+)\s*(?:€/kg|€/l)",
]


def extraer_precio(texto: str) -> Optional[float]:
    """Extrae el precio del texto."""
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
    """Extrae el precio por kg/L/ud."""
    for patron in _PATRONES_PRECIO_KG:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            try:
                valor = float(m.group(1).replace(",", "."))
                unidad = m.group(2).lower() if len(m.groups()) > 1 else "kg"
                unidad = {"kilo": "kg", "litro": "L", "lt": "L", "l": "L"}.get(unidad, unidad)
                if 0.01 <= valor <= 9999.0:
                    return valor, unidad
            except ValueError:
                continue
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS
# ─────────────────────────────────────────────────────────────────────────────
def parsear_productos_pagina(soup: BeautifulSoup, diagnostico: bool = False) -> list[dict]:
    """Extrae productos de una página de categoría de Mercadona."""
    productos = []
    vistos = set()
    
    # Buscar todos los enlaces que parecen productos (/p/algo)
    patron_producto = re.compile(r"/p/[\w-]+(?:-\d+)?")
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not patron_producto.search(href):
            continue
        
        if href in vistos:
            continue
        vistos.add(href)
        
        # Extraer nombre
        nombre = None
        # Buscar en el texto del enlace o en elementos cercanos
        texto_enlace = a.get_text(" ", strip=True)
        
        # El nombre suele estar en un span o div dentro del enlace
        nombre_elem = a.find(["span", "div", "h2", "h3", "h4"], 
                              class_=re.compile(r"(name|title|nombre)", re.I))
        if nombre_elem:
            nombre = nombre_elem.get_text(strip=True)
        
        if not nombre and texto_enlace:
            # Limpiar el texto del enlace (eliminar precios)
            nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", texto_enlace).strip()
        
        if not nombre or len(nombre) < 4:
            continue
        
        # Limpiar nombre
        nombre = re.sub(r"\s+", " ", nombre).strip()
        
        # Extraer precio del texto circundante
        # Buscar en el elemento padre
        padre = a.find_parent(["div", "article", "li"])
        texto_completo = padre.get_text(" ", strip=True) if padre else texto_enlace
        
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
    
    # Si no encontramos productos, intentar con contenedores alternativos
    if not productos:
        contenedores = soup.find_all(["div", "article"], class_=re.compile(r"(product|item|card)", re.I))
        
        for contenedor in contenedores:
            # Buscar nombre
            nombre_elem = contenedor.find(["h2", "h3", "h4", "span"], 
                                          class_=re.compile(r"(name|title|nombre)", re.I))
            if not nombre_elem:
                continue
            
            nombre = nombre_elem.get_text(strip=True)
            if not nombre or len(nombre) < 4:
                continue
            
            # Buscar precio
            texto_completo = contenedor.get_text(" ", strip=True)
            precio = extraer_precio(texto_completo)
            if precio is None:
                continue
            
            precio_kg, unidad_precio = extraer_precio_kg(texto_completo)
            
            # Buscar enlace
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
    
    return productos


def get_total_paginas(soup: BeautifulSoup) -> int:
    """Detecta el número total de páginas."""
    max_page = 0
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            try:
                num = int(m.group(1))
                max_page = max(max_page, num)
            except ValueError:
                continue
    
    # También buscar texto como "Página 1 de 3"
    texto = soup.get_text()
    m = re.search(r"[Pp]ágina\s+\d+\s+de\s+(\d+)", texto)
    if m:
        try:
            max_page = max(max_page, int(m.group(1)))
        except ValueError:
            pass
    
    return max_page if max_page > 0 else 1


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_KEY en .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def cargar_tienda_mercadona(sb: Client) -> Optional[str]:
    """Obtiene el ID de la tienda Mercadona en Supabase."""
    try:
        res = sb.table("tiendas").select("id").eq("nombre", "Mercadona").execute()
        if res.data:
            return res.data[0]["id"]
        else:
            log.error("Tienda 'Mercadona' no encontrada en Supabase")
            return None
    except Exception as e:
        log.error(f"Error cargando tienda: {e}")
        return None


def cargar_productos(sb: Client, page_size: int = 1000) -> list[dict]:
    """Carga productos existentes para matching fuzzy."""
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
            log.error(f"Error cargando productos: {e}")
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
            log.error(f"Error insertando producto '{nombre}': {e}")
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
            log.error(f"Error upsert precio: {e}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER DE CATEGORÍA
# ─────────────────────────────────────────────────────────────────────────────
def scrape_categoria(
    sb: Client,
    categoria: dict,
    tienda_id: str,
    productos_db: list[dict],
    stats: dict,
    modo_test: bool = False,
    diagnostico: bool = False,
):
    """Procesa todas las páginas de una categoría."""
    url_base = categoria["url"]
    cat_app = categoria["cat_app"]
    nombre_cat = categoria["nombre"]

    log.info(f"  📂 {nombre_cat} [{cat_app}]")

    soup = fetch(url_base, diagnostico=diagnostico)
    if not soup:
        stats["categorias_error"] += 1
        return

    total_pags = get_total_paginas(soup)
    log.info(f"     Páginas: {total_pags}")
    
    if modo_test:
        total_pags = min(total_pags, 1)

    for page_num in range(1, total_pags + 1):
        url = f"{url_base}?page={page_num}" if page_num > 1 else url_base
        soup_pag = soup if page_num == 1 else fetch(url, diagnostico=diagnostico)
        
        if not soup_pag:
            stats["paginas_error"] += 1
            continue

        items = parsear_productos_pagina(soup_pag, diagnostico=diagnostico)
        log.info(f"     Página {page_num}/{total_pags} → {len(items)} productos")

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
                    sb, item["nombre"], cat_app, nombre_cat, tienda_id
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
def main(modo_test: bool = False, diagnostico: bool = False):
    inicio_ts = datetime.now()
    log.info("=" * 65)
    log.info(f"🛒 Scraper Mercadona — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)

    try:
        sb = get_supabase()
        log.info("✅ Conectado a Supabase")
    except ValueError as e:
        log.error(f"❌ {e}")
        sys.exit(1)

    tienda_id = cargar_tienda_mercadona(sb)
    if not tienda_id:
        log.error("❌ No se encontró la tienda Mercadona en Supabase")
        sys.exit(1)

    productos_db = cargar_productos(sb)

    stats = {
        "procesados":       0,
        "actualizados":     0,
        "nuevos":           0,
        "sin_precio":       0,
        "errores_db":       0,
        "paginas_error":    0,
        "categorias_error": 0,
    }

    categorias = CATEGORIAS_MERCADONA
    
    if modo_test:
        categorias = categorias[:3]
        log.info(f"Modo test: {len(categorias)} categorías")

    for i, categoria in enumerate(categorias, 1):
        log.info(f"\n  [{i}/{len(categorias)}] {categoria['nombre']}")
        scrape_categoria(
            sb, categoria, tienda_id, productos_db, stats, modo_test, diagnostico
        )
        time.sleep(SLEEP_CATEGORIA)

    duracion = datetime.now() - inicio_ts
    log.info("=" * 65)
    log.info(f"✅ Scraper Mercadona finalizado en {duracion}")
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
        Path("scraper_mercadona_resumen.json").write_text(
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
        description="Scraper exclusivo para Mercadona en RadarSuper → Supabase",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Modo test: 3 categorías, 1 página cada una",
    )
    parser.add_argument(
        "--diagnostico",
        action="store_true",
        help="Guarda HTML de páginas problemáticas",
    )
    args = parser.parse_args()

    main(modo_test=args.test, diagnostico=args.diagnostico)
