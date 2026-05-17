"""
scraper_radarsuper.py - Scraper en dos fases para Mercadona y Carrefour
Fase 1: Extrae todos los enlaces de productos de las categorías
Fase 2: Scrapea cada producto individualmente

MEJORAS ANTI-BLOQUEO:
- Reintentos con backoff exponencial (espera progresiva)
- Manejo de errores HTTP 429 (Rate Limit)
- Timeouts configurables
- Rotación de User-Agent
- Pausas aleatorias entre peticiones
- Guardado de progreso para reanudar
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
import subprocess
import tempfile
import random
from datetime import date, datetime
from pathlib import Path
from typing import Optional, List

from bs4 import BeautifulSoup
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

# TIEMPOS DE ESPERA (aumentados para evitar bloqueos)
SLEEP_PAGINA    = 3.0      # Entre páginas
SLEEP_CATEGORIA = 5.0      # Entre categorías
SLEEP_PRODUCTO  = 1.0      # Entre productos

# CONFIGURACIÓN DE REINTENTOS
MAX_RETRIES = 5             # Número máximo de reintentos por URL
BASE_BACKOFF = 2.0          # Backoff exponencial: 2, 4, 8, 16, 32 segundos
REQUEST_TIMEOUT = 90        # Timeout global para cada petición

# CONFIGURACIÓN DE PAUSAS ALEATORIAS
MIN_RANDOM_SLEEP = 0.5      # Pausa mínima entre peticiones
MAX_RANDOM_SLEEP = 2.0      # Pausa máxima entre peticiones

DIAG_DIR = Path("diagnostico_radarsuper")

# CADENAS SOPORTADAS
CADENAS_SOPORTADAS = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
}

# USER-AGENTS PARA ROTAR
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
]

# MAPEO DE CATEGORÍAS
CATEGORIA_MAP = {
    "aceite-especias-salsas": "condimentos",
    "aceite-vinagre-sal": "condimentos",
    "bebe": "higiene",
    "carne": "carne",
    "pescado": "pescado",
    "fruta-verdura": "fruta",
    "lacteos": "lácteos",
    "pan": "pan",
    "congelados": "congelados",
    "bebidas": "bebidas",
    "snacks": "snacks",
    "dulces": "dulces",
    "higiene": "higiene",
    "limpieza": "limpieza",
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
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────
def random_sleep():
    """Pausa aleatoria para simular comportamiento humano."""
    sleep_time = random.uniform(MIN_RANDOM_SLEEP, MAX_RANDOM_SLEEP)
    time.sleep(sleep_time)


def get_random_user_agent() -> str:
    """Devuelve un User-Agent aleatorio."""
    return random.choice(USER_AGENTS)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP CON CURL (CON REINTENTOS Y BACKOFF EXPONENCIAL)
# ─────────────────────────────────────────────────────────────────────────────
_cookie_jar = None

def get_cookie_jar():
    global _cookie_jar
    if _cookie_jar is None:
        _cookie_jar = tempfile.NamedTemporaryFile(delete=False)
        log.info(f"🍪 Cookie jar creada")
    return _cookie_jar.name


def fetch_with_curl(url: str, diagnostico: bool = False, retry_count: int = 0) -> Optional[str]:
    """Usa curl con cookies persistentes, reintentos y backoff exponencial."""
    cookie_jar_path = get_cookie_jar()
    user_agent = get_random_user_agent()
    
    cmd = [
        'curl', '-s', '-L',
        '--user-agent', user_agent,
        '--cookie-jar', cookie_jar_path,
        '--cookie', cookie_jar_path,
        '--retry', '3',
        '--retry-delay', '2',
        '--connect-timeout', '30',
        '--max-time', str(REQUEST_TIMEOUT),
        '--compressed',
        '--header', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        '--header', 'Accept-Language: es-ES,es;q=0.9',
        '--header', 'Accept-Encoding: gzip, deflate, br',
        '--header', 'Connection: keep-alive',
        '--header', 'Upgrade-Insecure-Requests: 1',
        '--header', 'Sec-Fetch-Dest: document',
        '--header', 'Sec-Fetch-Mode: navigate',
        '--header', 'Sec-Fetch-Site: none',
        '--header', 'Sec-Fetch-User: ?1',
        url
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=REQUEST_TIMEOUT + 10)
        
        # Verificar si hay error de conexión o timeout
        if result.returncode != 0:
            raise subprocess.SubprocessError(f"curl return code: {result.returncode}, stderr: {result.stderr}")
        
        html = result.stdout
        
        # Verificar si Cloudflare nos bloqueó
        if "Attention Required" in html or "cf-challenge" in html or "cf-browser-verification" in html:
            log.warning(f"⚠️ Cloudflare challenge detectado en {url}")
            if diagnostico:
                _guardar_diagnostico(url, html)
            return None
        
        # Verificar que el HTML tiene contenido significativo
        if len(html) < 500:
            log.warning(f"⚠️ HTML demasiado corto ({len(html)} bytes) en {url}")
            return None
        
        return html
        
    except subprocess.TimeoutExpired:
        log.warning(f"Timeout en curl para {url}")
        if retry_count < MAX_RETRIES:
            wait_time = (BASE_BACKOFF ** retry_count) + random.uniform(0, 1)
            log.info(f"Reintento {retry_count + 1}/{MAX_RETRIES} en {wait_time:.1f}s")
            time.sleep(wait_time)
            return fetch_with_curl(url, diagnostico, retry_count + 1)
        return None
        
    except Exception as e:
        log.error(f"curl error: {e}")
        if retry_count < MAX_RETRIES:
            wait_time = (BASE_BACKOFF ** retry_count) + random.uniform(0, 1)
            log.info(f"Reintento {retry_count + 1}/{MAX_RETRIES} en {wait_time:.1f}s")
            time.sleep(wait_time)
            return fetch_with_curl(url, diagnostico, retry_count + 1)
        return None


def fetch(url: str, diagnostico: bool = False) -> Optional[BeautifulSoup]:
    """Descarga una página con reintentos y backoff exponencial."""
    html = fetch_with_curl(url, diagnostico=diagnostico)
    if html is None:
        return None
    if diagnostico:
        _guardar_diagnostico(url, html)
    return BeautifulSoup(html, 'html.parser')


def _guardar_diagnostico(url: str, html: str):
    """Guarda HTML para depuración."""
    try:
        DIAG_DIR.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre = hashlib.md5(url.encode()).hexdigest()[:8]
        path = DIAG_DIR / f"{ts}_{nombre}.html"
        path.write_text(html[:500000], encoding="utf-8")
        log.warning(f"🔍 HTML guardado: {path}")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRECIO
# ─────────────────────────────────────────────────────────────────────────────
def extraer_precio(texto: str) -> Optional[float]:
    """Extrae el precio del texto usando múltiples patrones."""
    patrones = [
        r"(\d{1,4}[.,]\d{2})\s*€",
        r"€\s*(\d{1,4}[.,]\d{2})",
        r"(\d{1,4}[.,]\d{2})\s*EUR",
        r'data-price=["\']?(\d{1,4}[.,]\d{2})',
        r'content=["\'](\d{1,4}[.,]\d{2})',
    ]
    for patron in patrones:
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
    patrones = [
        r"(\d{1,4}[.,]\d+)\s*€\s*/\s*(kg|Kg|K|L|l|lt|ud|unidad)",
        r"(\d{1,4}[.,]\d+)\s*€/(kg|l|ud)",
        r"(\d{1,4}[.,]\d+)\s*(?:€/kg|€/l|€/ud)",
    ]
    for patron in patrones:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            try:
                valor = float(m.group(1).replace(",", "."))
                unidad = m.group(2).lower() if len(m.groups()) > 1 else "kg"
                unidad = {"kilo": "kg", "litro": "L", "lt": "L", "l": "L", "unidad": "ud"}.get(unidad, unidad)
                if 0.01 <= valor <= 9999.0:
                    return valor, unidad
            except ValueError:
                continue
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1: EXTRAER CATEGORÍAS
# ─────────────────────────────────────────────────────────────────────────────
def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """Extrae todas las categorías de una cadena."""
    url_principal = f"{BASE_URL}/{cadena_slug}"
    soup = fetch(url_principal, diagnostico=diagnostico)
    
    if not soup:
        log.error(f"No se pudo acceder a {url_principal}")
        return []
    
    categorias = []
    vistos = set()
    patron = re.compile(rf"/{cadena_slug}/c/([\w-]+)-(\d+)")
    
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        match = patron.search(href)
        
        if match and href not in vistos:
            vistos.add(href)
            texto = a.get_text(strip=True)
            texto_limpio = re.sub(r"\s*\(\d+\)\s*$", "", texto).strip()
            
            if texto_limpio:
                url_completa = BASE_URL + href if href.startswith("/") else href
                slug_cat = match.group(1)
                cat_app = CATEGORIA_MAP.get(slug_cat, "general")
                
                categorias.append({
                    "slug": slug_cat,
                    "nombre": texto_limpio,
                    "url": url_completa,
                    "cat_app": cat_app,
                })
    
    log.info(f"  → {len(categorias)} categorías encontradas para {cadena_slug}")
    return categorias


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1: EXTRAER ENLACES DE PRODUCTOS
# ─────────────────────────────────────────────────────────────────────────────
def extraer_enlaces_productos_de_categoria(soup: BeautifulSoup, cadena_slug: str) -> List[str]:
    """Extrae todos los enlaces de productos de una página de categoría."""
    enlaces = []
    patron = re.compile(rf"/{cadena_slug}/p/[\w-]+")
    
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if patron.search(href):
            url_completa = BASE_URL + href if href.startswith("/") else href
            if url_completa not in enlaces:
                enlaces.append(url_completa)
    
    return enlaces


def fase1_extraer_todos_los_enlaces(cadena_slug: str, diagnostico: bool = False) -> List[str]:
    """Fase 1: Extrae todos los enlaces de productos de todas las categorías."""
    todos_los_enlaces = []
    
    categorias = extraer_categorias(cadena_slug, diagnostico=diagnostico)
    log.info(f"📂 Procesando {len(categorias)} categorías para extraer enlaces...")
    
    for i, categoria in enumerate(categorias, 1):
        log.info(f"  [{i}/{len(categorias)}] {categoria['nombre']}")
        
        soup = fetch(categoria["url"], diagnostico=diagnostico)
        if not soup:
            continue
        
        enlaces = extraer_enlaces_productos_de_categoria(soup, cadena_slug)
        log.info(f"      → {len(enlaces)} enlaces de productos encontrados")
        todos_los_enlaces.extend(enlaces)
        
        # Pausa aleatoria entre categorías
        random_sleep()
        time.sleep(SLEEP_CATEGORIA)
    
    todos_los_enlaces = list(set(todos_los_enlaces))
    log.info(f"✅ Total de enlaces únicos para {cadena_slug}: {len(todos_los_enlaces)}")
    
    enlaces_file = f"enlaces_{cadena_slug}.json"
    with open(enlaces_file, "w", encoding="utf-8") as f:
        json.dump(todos_los_enlaces, f, indent=2, ensure_ascii=False)
    log.info(f"💾 Enlaces guardados en {enlaces_file}")
    
    return todos_los_enlaces


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2: SCRAPEAR PRODUCTOS
# ─────────────────────────────────────────────────────────────────────────────
def scrapear_producto_desde_url(url: str, diagnostico: bool = False) -> Optional[dict]:
    """Extrae datos de un producto desde su URL individual."""
    soup = fetch(url, diagnostico=diagnostico)
    if not soup:
        return None
    
    try:
        # Nombre del producto (h1)
        nombre_elem = soup.find("h1")
        nombre = nombre_elem.get_text(strip=True) if nombre_elem else None
        
        # Precio principal
        precio = None
        if nombre_elem:
            siguiente = nombre_elem.find_next()
            if siguiente:
                precio = extraer_precio(siguiente.get_text())
        
        if not precio:
            texto = soup.get_text()
            precio = extraer_precio(texto)
        
        # Precio por kg/L
        precio_kg, unidad = extraer_precio_kg(soup.get_text())
        
        # Extraer cadena de la URL
        cadena = "desconocida"
        if "mercadona" in url:
            cadena = "Mercadona"
        elif "carrefour" in url:
            cadena = "Carrefour"
        
        return {
            "cadena": cadena,
            "nombre": nombre,
            "precio": precio,
            "precio_kg": precio_kg,
            "unidad_precio": unidad,
            "url": url,
            "fecha_scraping": str(date.today())
        }
        
    except Exception as e:
        log.error(f"Error scrapeando producto {url}: {e}")
        return None


def fase2_scrapear_productos(cadena_slug: str, diagnostico: bool = False, limite: int = None):
    """Fase 2: Scrapea cada producto individualmente desde los enlaces guardados."""
    enlaces_file = f"enlaces_{cadena_slug}.json"
    
    try:
        with open(enlaces_file, "r", encoding="utf-8") as f:
            enlaces = json.load(f)
    except FileNotFoundError:
        log.error(f"No se encuentra {enlaces_file}. Ejecuta la fase 1 primero.")
        return
    
    # Verificar si ya hay progreso guardado
    progreso_file = f"progreso_{cadena_slug}.json"
    start_index = 0
    productos_scrapeados = []
    
    try:
        with open(progreso_file, "r", encoding="utf-8") as f:
            progreso = json.load(f)
            start_index = progreso.get("last_index", 0)
            productos_scrapeados = progreso.get("productos", [])
            log.info(f"🔄 Reanudando desde producto {start_index + 1}/{len(enlaces)}")
    except FileNotFoundError:
        pass
    
    if limite:
        enlaces = enlaces[:limite]
        log.info(f"🧪 Modo test: procesando {len(enlaces)} productos")
    
    log.info(f"🔄 Scrapeando {len(enlaces)} productos para {cadena_slug}...")
    
    for i, url in enumerate(enlaces[start_index:], start_index + 1):
        log.info(f"  [{i}/{len(enlaces)}] {url[:80]}...")
        
        producto = scrapear_producto_desde_url(url, diagnostico=diagnostico)
        if producto and producto["precio"]:
            productos_scrapeados.append(producto)
            log.info(f"      ✅ {producto['nombre'][:50]} - {producto['precio']}€")
        else:
            log.warning(f"      ❌ No se pudo scrapear o sin precio")
        
        # Guardar progreso cada 10 productos
        if i % 10 == 0:
            with open(progreso_file, "w", encoding="utf-8") as f:
                json.dump({"last_index": i, "productos": productos_scrapeados}, f, indent=2, ensure_ascii=False)
            # Guardar copia parcial
            with open(f"productos_{cadena_slug}_parcial.json", "w", encoding="utf-8") as f:
                json.dump(productos_scrapeados, f, indent=2, ensure_ascii=False)
        
        # Pausa aleatoria entre productos
        random_sleep()
        time.sleep(SLEEP_PRODUCTO)
    
    output_file = f"productos_{cadena_slug}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(productos_scrapeados, f, indent=2, ensure_ascii=False)
    
    # Limpiar archivo de progreso
    if os.path.exists(progreso_file):
        os.remove(progreso_file)
    
    log.info(f"✅ Scraping completado. {len(productos_scrapeados)} productos guardados en {output_file}")
    
    return productos_scrapeados


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Scraper RadarSuper en dos fases con anti-bloqueo")
    parser.add_argument("--cadena", choices=list(CADENAS_SOPORTADAS.keys()) + ["todas"], 
                        default="mercadona", help="Cadena a procesar")
    parser.add_argument("--fase", choices=["1", "2", "ambas"], default="ambas",
                        help="Fase: 1 (extraer enlaces), 2 (scrapear productos)")
    parser.add_argument("--test", action="store_true", help="Modo test (limitar a 10 productos)")
    parser.add_argument("--diagnostico", action="store_true", help="Guardar HTML para depuración")
    parser.add_argument("--slow", action="store_true", help="Modo lento (pausas más largas para evitar bloqueos)")
    args = parser.parse_args()
    
    # Ajustar tiempos si está en modo lento
    global SLEEP_PAGINA, SLEEP_CATEGORIA, SLEEP_PRODUCTO, MIN_RANDOM_SLEEP, MAX_RANDOM_SLEEP
    if args.slow:
        SLEEP_PAGINA = 10.0
        SLEEP_CATEGORIA = 15.0
        SLEEP_PRODUCTO = 3.0
        MIN_RANDOM_SLEEP = 2.0
        MAX_RANDOM_SLEEP = 5.0
        log.info("🐢 Modo lento activado - pausas más largas")
    
    inicio_ts = datetime.now()
    log.info("=" * 65)
    log.info(f"🛒 Scraper RadarSuper v3 (anti-bloqueo) — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)
    
    # Determinar cadenas a procesar
    if args.cadena and args.cadena != "todas":
        cadenas = {args.cadena: CADENAS_SOPORTADAS[args.cadena]}
    else:
        cadenas = CADENAS_SOPORTADAS
    
    for cadena_slug, cadena_nombre in cadenas.items():
        log.info(f"\n🏪 Procesando {cadena_nombre} ({cadena_slug})")
        
        if args.fase in ["1", "ambas"]:
            log.info(f"\n📌 FASE 1: Extrayendo enlaces de productos...")
            fase1_extraer_todos_los_enlaces(cadena_slug, diagnostico=args.diagnostico)
        
        if args.fase in ["2", "ambas"]:
            log.info(f"\n📌 FASE 2: Scrapeando productos individualmente...")
            limite = 10 if args.test else None
            fase2_scrapear_productos(cadena_slug, diagnostico=args.diagnostico, limite=limite)
    
    duracion = datetime.now() - inicio_ts
    log.info("=" * 65)
    log.info(f"✅ Scraper finalizado en {duracion}")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
