"""
scraper_radarsuper.py - Versión con undetected-chromedriver
=============================================================
Scraper para radarsuper.com que evade Cloudflare usando un navegador real.
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

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from thefuzz import fuzz
from dotenv import load_dotenv
from supabase import create_client, Client
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL   = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")
CIUDAD_SCRAPER = os.getenv("CIUDAD_SCRAPER", "Nacional")

BASE_URL = "https://radarsuper.com"

FUZZY_THRESHOLD = 72

SLEEP_PAGINA    = 3.0  # Aumentado porque el navegador es más lento
SLEEP_CATEGORIA = 5.0
SLEEP_PRODUCTO  = 0.5

DIAG_DIR = Path("diagnostico_radarsuper")

# CADENAS SOPORTADAS
CADENAS_SOPORTADAS = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
    "dia": "Dia",
    "alcampo": "Alcampo",
    "lidl": "Lidl",
}

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
# NAVEGADOR CON undetected-chromedriver
# ─────────────────────────────────────────────────────────────────────────────
_driver = None

def get_driver() -> uc.Chrome:
    """Inicializa o devuelve el driver de Chrome ya configurado."""
    global _driver
    
    if _driver is None:
        log.info("🚀 Iniciando navegador Chrome (undetected-chromedriver)...")
        options = uc.ChromeOptions()
        
        # Configuración para evitar detección
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        
        # Modo headless (sin interfaz gráfica) - opcional
        # Descomenta la siguiente línea si quieres que no se vea el navegador
        # options.add_argument('--headless')
        
        # User-Agent real
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
        
        # Configuración adicional para pasar desapercibido
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--enable-features=NetworkService,NetworkServiceInProcess')
        
        _driver = uc.Chrome(options=options)
        _driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        log.info("✅ Navegador listo")
    
    return _driver


def fetch(url: str, diagnostico: bool = False) -> Optional[BeautifulSoup]:
    """
    Descarga una página usando undetected-chromedriver para evadir Cloudflare.
    """
    try:
        driver = get_driver()
        
        log.debug(f"Cargando: {url}")
        driver.get(url)
        
        # Esperar a que la página cargue completamente
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Espera adicional para que se ejecuten scripts
        time.sleep(3)
        
        html = driver.page_source
        
        if diagnostico:
            _guardar_diagnostico(url, html)
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Verificar si Cloudflare nos bloqueó
        if "Attention Required" in html or "cf-challenge" in html:
            log.error(f"❌ Cloudflare bloqueó la página: {url}")
            if diagnostico:
                _guardar_diagnostico(f"bloqueado_{url}", html)
            return None
        
        return soup
        
    except Exception as e:
        log.error(f"Error fetching {url}: {e}")
        if diagnostico:
            _guardar_diagnostico(f"error_{url}", str(e))
        return None


def cerrar_navegador():
    """Cierra el navegador al finalizar el scraper."""
    global _driver
    if _driver:
        log.info("🔒 Cerrando navegador...")
        _driver.quit()
        _driver = None


def _guardar_diagnostico(url: str, html: str):
    """Guarda HTML para depuración."""
    try:
        DIAG_DIR.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre = hashlib.md5(url.encode()).hexdigest()[:8]
        path = DIAG_DIR / f"{ts}_{nombre}.html"
        path.write_text(html[:500000], encoding="utf-8")  # Limitar tamaño
        log.warning(f"🔍 HTML guardado: {path}")
    except Exception as e:
        log.debug(f"Error guardando diagnóstico: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRECIO
# ─────────────────────────────────────────────────────────────────────────────
def extraer_precio(texto: str) -> Optional[float]:
    """Extrae el precio del texto."""
    patrones = [
        r"(\d{1,4}[.,]\d{2})\s*€",
        r"€\s*(\d{1,4}[.,]\d{2})",
        r"(\d{1,4}[.,]\d{2})\s*EUR",
    ]
    for patron in patrones:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except ValueError:
                continue
    return None


def extraer_precio_kg(texto: str) -> tuple[Optional[float], Optional[str]]:
    """Extrae el precio por kg/L/ud."""
    patron = r"(\d{1,4}[.,]\d+)\s*€\s*/\s*(kg|Kg|L|l|ud|unidad)"
    m = re.search(patron, texto, re.IGNORECASE)
    if m:
        try:
            valor = float(m.group(1).replace(",", "."))
            unidad = m.group(2).lower()
            return valor, unidad
        except ValueError:
            pass
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE CATEGORÍAS
# ─────────────────────────────────────────────────────────────────────────────
def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """
    Extrae automáticamente las categorías desde la página principal de la cadena.
    """
    url_principal = f"{BASE_URL}/{cadena_slug}"
    soup = fetch(url_principal, diagnostico=diagnostico)
    
    if not soup:
        log.error(f"No se pudo acceder a {url_principal}")
        return []
    
    categorias = []
    vistos = set()
    
    # Patrón para encontrar categorías
    patron = re.compile(rf"/{cadena_slug}/c/([\w-]+)-\d+")
    
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
    
    if not categorias:
        log.error(f"No se encontraron categorías para {cadena_slug}")
        return []
    
    log.info(f"  → {len(categorias)} categorías encontradas")
    return categorias


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS
# ─────────────────────────────────────────────────────────────────────────────
def parsear_productos_pagina(soup: BeautifulSoup, diagnostico: bool = False) -> list[dict]:
    """
    Extrae productos de una página de categoría.
    """
    productos = []
    vistos = set()
    
    # Buscar cualquier enlace que contenga /p/ (formato de producto)
    patron_producto = re.compile(r"/p/[\w-]+(?:-\d+)?")
    
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        
        # Verificar que sea un enlace de producto
        if not patron_producto.search(href):
            continue
        
        if href in vistos:
            continue
        
        # Obtener texto del enlace y contexto
        texto = a.get_text(" ", strip=True)
        
        # Verificar que tenga precio
        precio = extraer_precio(texto)
        if precio is None:
            # Buscar en el elemento padre
            padre = a.find_parent(["div", "article", "li"])
            if padre:
                precio = extraer_precio(padre.get_text(" ", strip=True))
        
        if precio is None:
            continue
        
        # Extraer nombre (todo antes del precio)
        nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", texto).strip()
        if not nombre or len(nombre) < 4:
            # Intentar obtener nombre de un elemento de título cercano
            titulo = a.find(["h2", "h3", "h4", "span"])
            if titulo:
                nombre = titulo.get_text(strip=True)
        
        if not nombre or len(nombre) < 4:
            continue
        
        # Limpiar nombre
        nombre = re.sub(r"Ver producto|Comprar|Más información", "", nombre, flags=re.I).strip()
        nombre = re.sub(r"\s+", " ", nombre).strip()
        
        vistos.add(href)
        url_producto = BASE_URL + href if href.startswith("/") else href
        precio_kg, unidad = extraer_precio_kg(texto)
        
        productos.append({
            "nombre": nombre[:150],
            "precio": precio,
            "precio_kg": precio_kg,
            "unidad_precio": unidad,
            "url": url_producto,
        })
    
    if not productos and diagnostico:
        _guardar_diagnostico("pagina_sin_productos", str(soup))
        log.warning("⚠️ No se encontraron productos")
    
    return productos


def get_total_paginas(soup: BeautifulSoup) -> int:
    """Detecta el número total de páginas."""
    max_page = 0
    
    # Buscar en enlaces de paginación
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            try:
                max_page = max(max_page, int(m.group(1)))
            except ValueError:
                continue
    
    # Buscar texto como "Página 1 de 3"
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


def cargar_tienda(sb: Client, nombre: str) -> Optional[str]:
    """Obtiene el ID de una tienda por su nombre."""
    try:
        res = sb.table("tiendas").select("id").eq("nombre", nombre).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.error(f"Error cargando tienda {nombre}: {e}")
        return None


def cargar_productos(sb: Client) -> list[dict]:
    """Carga los productos existentes para el matching fuzzy."""
    try:
        res = sb.table("productos").select("id, nombre").execute()
        return res.data
    except Exception as e:
        log.error(f"Error cargando productos: {e}")
        return []


def buscar_producto_fuzzy(nombre: str, productos: list[dict]) -> tuple[Optional[str], int]:
    """Busca un producto por similitud de nombre."""
    mejor_score = 0
    mejor_id = None
    
    for p in productos:
        score = fuzz.token_set_ratio(nombre.lower(), p["nombre"].lower())
        if score > mejor_score:
            mejor_score = score
            mejor_id = p["id"]
    
    if mejor_score >= FUZZY_THRESHOLD:
        return mejor_id, mejor_score
    return None, mejor_score


def insertar_producto(
    sb: Client, nombre: str, categoria: str, subcategoria: str, tienda_id: str
) -> Optional[str]:
    """Inserta un nuevo producto en Supabase."""
    try:
        res = sb.table("productos").insert({
            "nombre": nombre,
            "categoria": categoria,
            "subcategoria": subcategoria,
            "tienda_origen": tienda_id,
            "verificado": False
        }).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.error(f"Error insertando producto: {e}")
        return None


def upsert_precio(sb: Client, producto_id: str, tienda_id: str, precio: float):
    """Inserta o actualiza el precio de un producto."""
    try:
        sb.table("precios").upsert({
            "producto_id": producto_id,
            "tienda_id": tienda_id,
            "ciudad": CIUDAD_SCRAPER,
            "precio": precio,
            "fecha": str(date.today()),
            "fuente": "radarsuper"
        }, on_conflict="producto_id,tienda_id,ciudad,fecha").execute()
    except Exception as e:
        log.error(f"Error upsert precio: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER POR CATEGORÍA
# ─────────────────────────────────────────────────────────────────────────────
def scrape_categoria(
    sb: Client,
    cadena_nombre: str,
    categoria: dict,
    tienda_id: str,
    productos_db: list[dict],
    stats: dict,
    modo_test: bool = False,
    diagnostico: bool = False,
):
    """Procesa todas las páginas de una categoría."""
    url_base = categoria["url"]
    log.info(f"  📂 {categoria['nombre']} [{categoria['cat_app']}]")

    soup = fetch(url_base, diagnostico=diagnostico)
    if not soup:
        stats["categorias_error"] += 1
        return

    total_pags = 1 if modo_test else get_total_paginas(soup)
    log.info(f"     Páginas: {total_pags}")

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

            producto_id, score = buscar_producto_fuzzy(item["nombre"], productos_db)

            if producto_id:
                stats["actualizados"] += 1
                log.debug(f"Match ({score}%): '{item['nombre']}'")
            else:
                log.info(f"Nuevo producto: '{item['nombre']}'")
                producto_id = insertar_producto(
                    sb, item["nombre"], categoria["cat_app"], categoria["nombre"], tienda_id
                )
                if not producto_id:
                    stats["errores_db"] += 1
                    continue
                productos_db.append({"id": producto_id, "nombre": item["nombre"]})
                stats["nuevos"] += 1

            upsert_precio(sb, producto_id, tienda_id, item["precio"])
            time.sleep(SLEEP_PRODUCTO)

        time.sleep(SLEEP_PAGINA)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(cadena_seleccionada: Optional[str] = None, modo_test: bool = False, diagnostico: bool = False):
    inicio_ts = datetime.now()
    log.info("=" * 65)
    log.info(f"🛒 Scraper RadarSuper (con anti-bloqueo) — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 65)

    try:
        sb = get_supabase()
        log.info("✅ Conectado a Supabase")
    except ValueError as e:
        log.error(f"❌ {e}")
        sys.exit(1)

    # Determinar qué cadenas procesar
    if cadena_seleccionada and cadena_seleccionada != "todas":
        if cadena_seleccionada not in CADENAS_SOPORTADAS:
            log.error(f"❌ Cadena '{cadena_seleccionada}' no soportada")
            cerrar_navegador()
            sys.exit(1)
        cadenas_a_procesar = {cadena_seleccionada: CADENAS_SOPORTADAS[cadena_seleccionada]}
    else:
        cadenas_a_procesar = CADENAS_SOPORTADAS

    productos_globales = cargar_productos(sb)
    
    stats_globales = {
        "procesados": 0, "actualizados": 0, "nuevos": 0,
        "sin_precio": 0, "errores_db": 0, "paginas_error": 0, "categorias_error": 0
    }

    try:
        for cadena_slug, cadena_nombre in cadenas_a_procesar.items():
            log.info(f"\n🏪 Procesando {cadena_nombre} ({cadena_slug})...")
            
            tienda_id = cargar_tienda(sb, cadena_nombre)
            if not tienda_id:
                log.warning(f"⚠️ Tienda '{cadena_nombre}' no encontrada en Supabase")
                continue

            categorias = extraer_categorias(cadena_slug, diagnostico=diagnostico)
            if not categorias:
                log.error(f"No se encontraron categorías para {cadena_nombre}")
                continue

            if modo_test:
                categorias = categorias[:3]
                log.info(f"Modo test: {len(categorias)} categorías")

            for i, categoria in enumerate(categorias, 1):
                log.info(f"\n  [{i}/{len(categorias)}] {categoria['nombre']}")
                scrape_categoria(
                    sb, cadena_nombre, categoria, tienda_id, productos_globales,
                    stats_globales, modo_test, diagnostico
                )
                time.sleep(SLEEP_CATEGORIA)
    finally:
        # Siempre cerrar el navegador, incluso si hay error
        cerrar_navegador()

    duracion = datetime.now() - inicio_ts
    log.info("=" * 65)
    log.info(f"✅ Scraper finalizado en {duracion}")
    log.info(f"   Productos procesados: {stats_globales['procesados']}")
    log.info(f"   Precios actualizados: {stats_globales['actualizados']}")
    log.info(f"   Productos nuevos: {stats_globales['nuevos']}")
    log.info(f"   Sin precio: {stats_globales['sin_precio']}")
    log.info(f"   Errores DB: {stats_globales['errores_db']}")
    log.info(f"   Páginas error: {stats_globales['paginas_error']}")
    log.info(f"   Categorías error: {stats_globales['categorias_error']}")
    log.info("=" * 65)

    # Guardar resumen
    try:
        resumen = {
            "fecha": str(date.today()),
            "duracion": str(duracion),
            **stats_globales
        }
        Path("scraper_radarsuper_resumen.json").write_text(
            json.dumps(resumen, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper RadarSuper con anti-bloqueo")
    parser.add_argument("--cadena", choices=list(CADENAS_SOPORTADAS.keys()) + ["todas"], 
                        default="todas", help="Cadena a procesar")
    parser.add_argument("--test", action="store_true", help="Modo test (3 categorías, 1 página)")
    parser.add_argument("--diagnostico", action="store_true", help="Guardar HTML para depuración")
    args = parser.parse_args()

    main(cadena_seleccionada=args.cadena, modo_test=args.test, diagnostico=args.diagnostico)
