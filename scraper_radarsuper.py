"""
scraper_radarsuper.py - Versión con curl y cookies persistentes
Para ejecutar en GitHub Actions sin dependencias pesadas
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
from datetime import date, datetime
from pathlib import Path
from typing import Optional

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

SLEEP_PAGINA    = 2.0
SLEEP_CATEGORIA = 3.0
SLEEP_PRODUCTO  = 0.1

DIAG_DIR = Path("diagnostico_radarsuper")

# CADENAS SOPORTADAS
CADENAS_SOPORTADAS = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
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
    "especias": "condimentos",
    "mayonesa-ketchup-mostaza": "condimentos",
    "otras-salsas": "condimentos",
    "agua-refrescos": "bebidas",
    "zumos": "bebidas",
    "aperitivos": "snacks",
    "patatas-fritas-snacks": "snacks",
    "arroz-legumbres-pasta": "pasta",
    "azucar-caramelos-chocolate": "dulces",
    "cacao-cafe-e-infusiones": "cafe",
    "cereales-galletas": "galletas",
    "charcuteria-quesos": "charcuteria",
    "conservas-caldos-cremas": "conservas",
    "huevos-leche-mantequilla": "lacteos",
    "limpieza-hogar": "limpieza",
    "mascotas": "mascotas",
    "panaderia-pasteleria": "pan",
    "pizzas-platos-preparados": "preparados",
    "postres-yogures": "lacteos",
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
# HTTP CON CURL (con cookies persistentes)
# ─────────────────────────────────────────────────────────────────────────────
_cookie_jar = None

def get_cookie_jar():
    """Obtiene o crea un archivo temporal para las cookies."""
    global _cookie_jar
    if _cookie_jar is None:
        _cookie_jar = tempfile.NamedTemporaryFile(delete=False)
        log.info(f"🍪 Cookie jar creada: {_cookie_jar.name}")
    return _cookie_jar.name


def fetch_with_curl(url: str, diagnostico: bool = False) -> Optional[str]:
    """Usa curl con cookies persistentes para simular navegador real."""
    cookie_jar_path = get_cookie_jar()
    
    cmd = [
        'curl', '-s', '-L',
        '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        '--cookie-jar', cookie_jar_path,
        '--cookie', cookie_jar_path,
        '--retry', '3',
        '--retry-delay', '2',
        '--connect-timeout', '30',
        '--max-time', '60',
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
        log.debug(f"🌐 curl: {url}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
        
        if result.returncode == 0 and len(result.stdout) > 500:
            html = result.stdout
            
            # Verificar si Cloudflare nos bloqueó
            if "Attention Required" in html or "cf-challenge" in html or "cf-browser-verification" in html:
                log.warning(f"⚠️ Cloudflare challenge detectado en {url}")
                if diagnostico:
                    _guardar_diagnostico(url, html)
                return None
            
            return html
        else:
            log.error(f"curl error: returncode={result.returncode}, stdout_len={len(result.stdout)}")
            return None
            
    except subprocess.TimeoutExpired:
        log.error(f"Timeout en curl para {url}")
        return None
    except Exception as e:
        log.error(f"curl exception: {e}")
        return None


def fetch(url: str, diagnostico: bool = False) -> Optional[BeautifulSoup]:
    """Descarga una página usando curl."""
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
    except Exception as e:
        log.debug(f"Error guardando diagnóstico: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRECIO
# ─────────────────────────────────────────────────────────────────────────────
def extraer_precio(texto: str) -> Optional[float]:
    """Extrae el precio del texto usando múltiples patrones."""
    patrones = [
        r"(\d{1,4}[.,]\d{2})\s*€",
        r"€\s*(\d{1,4}[.,]\d{2})",
        r"(\d{1,4}[.,]\d{2})\s*EUR",
        r"(\d{1,4}[.,]\d{2})\s*€\s*$",
        r"^(\d{1,4}[.,]\d{2})\s*€",
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
        r"(\d{1,4}[.,]\d+)\s*(?:€|€)\s*(?:por|el)\s*(kg|l|ud)",
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
# EXTRACCIÓN DE CATEGORÍAS
# ─────────────────────────────────────────────────────────────────────────────
def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """Extrae automáticamente las categorías desde la página principal."""
    url_principal = f"{BASE_URL}/{cadena_slug}"
    soup = fetch(url_principal, diagnostico=diagnostico)
    
    if not soup:
        log.error(f"No se pudo acceder a {url_principal}")
        return []
    
    categorias = []
    vistos = set()
    
    # Patrón para encontrar categorías: /mercadona/c/nombre-id
    patron = re.compile(rf"/{cadena_slug}/c/([\w-]+)-(\d+)")
    
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        match = patron.search(href)
        
        if match and href not in vistos:
            vistos.add(href)
            texto = a.get_text(strip=True)
            # Limpiar el texto: eliminar el contador de productos " (123)"
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
    """Extrae productos de una página de categoría usando múltiples estrategias."""
    productos = []
    vistos = set()
    
    # Mostrar estructura para diagnóstico
    if diagnostico:
        # Buscar cualquier enlace que contenga /p/
        enlaces_p = [a.get("href", "") for a in soup.find_all("a", href=True) if "/p/" in a.get("href", "")]
        if enlaces_p:
            log.info(f"   🔍 Enlaces /p/ encontrados: {len(enlaces_p)}")
            for href in enlaces_p[:3]:
                log.info(f"      - {href}")
        else:
            log.warning("   🔍 No se encontraron enlaces con /p/")
        
        # Buscar precios en el texto
        texto_pagina = soup.get_text()
        precios = re.findall(r"\d+[.,]\d+\s*€", texto_pagina)
        if precios:
            log.info(f"   🔍 Precios encontrados en texto: {precios[:3]}")
    
    # ESTRATEGIA 1: Buscar elementos con clase "product", "item" o "card"
    contenedores = soup.find_all(["div", "article", "li"], 
                                  class_=re.compile(r"(product|item|card|producto|result|product-card)", re.I))
    
    for contenedor in contenedores:
        # Buscar enlace a producto
        enlace = contenedor.find("a", href=re.compile(r"/p/|/producto/"))
        if not enlace:
            continue
        
        href = enlace.get("href", "")
        if href in vistos:
            continue
        
        # Extraer nombre
        nombre = None
        nombre_elem = contenedor.find(["h2", "h3", "h4", "span"], 
                                       class_=re.compile(r"(name|title|nombre|product)", re.I))
        if nombre_elem:
            nombre = nombre_elem.get_text(strip=True)
        
        if not nombre:
            nombre = enlace.get_text(strip=True)
        
        if not nombre or len(nombre) < 4:
            continue
        
        # Limpiar nombre
        nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", nombre).strip()
        nombre = re.sub(r"Ver producto|Comprar|Más información|Nuevo", "", nombre, flags=re.I).strip()
        nombre = re.sub(r"\s+", " ", nombre).strip()
        
        # Extraer precio
        texto = contenedor.get_text(" ", strip=True)
        precio = extraer_precio(texto)
        
        if precio is None:
            precio_elem = contenedor.find(class_=re.compile(r"(price|precio|cost)", re.I))
            if precio_elem:
                precio = extraer_precio(precio_elem.get_text(" ", strip=True))
        
        if precio is None:
            continue
        
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
    
    # ESTRATEGIA 2: Buscar directamente enlaces /p/
    if not productos:
        log.debug("Estrategia 1 no encontró productos, probando estrategia 2...")
        
        for a in soup.find_all("a", href=re.compile(r"/p/[\w-]+")):
            href = a.get("href", "")
            if href in vistos:
                continue
            
            texto = a.get_text(" ", strip=True)
            precio = extraer_precio(texto)
            
            if precio is None:
                padre = a.find_parent(["div", "article", "li"])
                if padre:
                    precio = extraer_precio(padre.get_text(" ", strip=True))
            
            if precio is None:
                continue
            
            nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", texto).strip()
            if not nombre or len(nombre) < 4:
                titulo = a.find(["h2", "h3", "h4"])
                if titulo:
                    nombre = titulo.get_text(strip=True)
            
            if not nombre or len(nombre) < 4:
                continue
            
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
    
    # ESTRATEGIA 3: Buscar cualquier enlace que contenga precio
    if not productos:
        log.debug("Estrategia 2 no encontró productos, probando estrategia 3...")
        
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            texto = a.get_text(" ", strip=True)
            precio = extraer_precio(texto)
            
            if precio is None:
                continue
            
            # Verificar que no sea una categoría
            if "/c/" in href or "/categoria" in href:
                continue
            
            nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", texto).strip()
            if not nombre or len(nombre) < 4:
                continue
            
            if href in vistos:
                continue
            
            vistos.add(href)
            url_producto = BASE_URL + href if href.startswith("/") else href
            productos.append({
                "nombre": nombre[:150],
                "precio": precio,
                "precio_kg": None,
                "unidad_precio": None,
                "url": url_producto,
            })
    
    if not productos and diagnostico:
        log.warning("⚠️ NO se encontraron productos - revisa el HTML guardado")
    
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
    
    # Buscar texto como "Mostrando 1–36 de 4359"
    texto = soup.get_text()
    m = re.search(r"Mostrando \d+[–-]\d+ de (\d+)", texto)
    if m:
        try:
            total_productos = int(m.group(1))
            max_page = max(max_page, (total_productos + 35) // 36)
        except ValueError:
            pass
    
    # Buscar "Página X de Y"
    m = re.search(r"[Pp]ágina\s+\d+\s+[Dd]e\s+(\d+)", texto)
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
    try:
        res = sb.table("tiendas").select("id").eq("nombre", nombre).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.error(f"Error cargando tienda {nombre}: {e}")
        return None


def cargar_productos(sb: Client) -> list[dict]:
    try:
        res = sb.table("productos").select("id, nombre").execute()
        return res.data
    except Exception as e:
        log.error(f"Error cargando productos: {e}")
        return []


def buscar_producto_fuzzy(nombre: str, productos: list[dict]) -> tuple[Optional[str], int]:
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
# SCRAPER
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
                log.info(f"Nuevo producto: '{item['nombre']}' (score: {score}%)")
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
    log.info(f"🛒 Scraper RadarSuper (curl) — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
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
            sys.exit(1)
        cadenas_a_procesar = {cadena_seleccionada: CADENAS_SOPORTADAS[cadena_seleccionada]}
    else:
        cadenas_a_procesar = CADENAS_SOPORTADAS

    productos_globales = cargar_productos(sb)
    
    stats_globales = {
        "procesados": 0, "actualizados": 0, "nuevos": 0,
        "sin_precio": 0, "errores_db": 0, "paginas_error": 0, "categorias_error": 0
    }

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
    parser = argparse.ArgumentParser(description="Scraper RadarSuper con curl")
    parser.add_argument("--cadena", choices=list(CADENAS_SOPORTADAS.keys()) + ["todas"], 
                        default="mercadona", help="Cadena a procesar")
    parser.add_argument("--test", action="store_true", help="Modo test (3 categorías, 1 página)")
    parser.add_argument("--diagnostico", action="store_true", help="Guardar HTML para depuración")
    args = parser.parse_args()

    main(cadena_seleccionada=args.cadena, modo_test=args.test, diagnostico=args.diagnostico)
