"""
scraper_radarsuper.py v3 - API Based
=====================================
Usa la API interna de radarsuper.com (mucho más robusta que HTML parsing)

Estrategia:
  - Obtiene catálogo de productos desde la API /api/v1/products
  - Filtra por cadena (Mercadona, Carrefour, etc.)
  - Mapea categorías automáticamente
  - Upsert en Supabase sin dependencia de HTML cambiante
"""

from __future__ import annotations

import os
import sys
import time
import json
import logging
import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
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

BASE_URL = "https://radarsuper.com/api/v1"

FUZZY_THRESHOLD = 72
SLEEP_ENTRE_REQUESTS = 0.5
MAX_REINTENTOS = 3
BACKOFF_BASE = 2.0

# Mapeo de cadenas
CADENAS_SOPORTADAS = {
    "mercadona": "Mercadona",
    "carrefour": "Carrefour",
    "dia": "Dia",
    "alcampo": "Alcampo",
    "lidl": "Lidl",
}

# Mapeo de categorías
CATEGORIA_MAP = {
    "aceite": "condimentos",
    "especias": "condimentos",
    "salsas": "condimentos",
    "agua": "bebidas",
    "refrescos": "bebidas",
    "bebidas": "bebidas",
    "cerveza": "bebidas",
    "vino": "bebidas",
    "cafe": "bebidas",
    "te": "bebidas",
    "zumos": "bebidas",
    "carne": "carne",
    "vacuno": "carne",
    "cerdo": "carne",
    "pollo": "carne",
    "aves": "carne",
    "pescado": "pescado",
    "marisco": "pescado",
    "fruta": "fruta",
    "verdura": "fruta",
    "lechuga": "fruta",
    "ensalada": "fruta",
    "leche": "lácteos",
    "lacteos": "lácteos",
    "queso": "lácteos",
    "yogur": "lácteos",
    "pan": "pan",
    "bolleria": "pan",
    "pasteleria": "pan",
    "harina": "pan",
    "congelados": "congelados",
    "helados": "congelados",
    "snacks": "snacks",
    "aperitivos": "snacks",
    "patatas": "snacks",
    "frutos": "snacks",
    "dulces": "dulces",
    "chocolate": "dulces",
    "caramelos": "dulces",
    "galletas": "galletas",
    "cereales": "galletas",
    "higiene": "higiene",
    "jabon": "higiene",
    "champu": "higiene",
    "cuidado": "higiene",
    "desodorante": "higiene",
    "pañales": "higiene",
    "toallitas": "higiene",
    "limpieza": "limpieza",
    "detergente": "limpieza",
    "lejia": "limpieza",
    "limpiahogar": "limpieza",
    "papel": "limpieza",
    "basura": "limpieza",
    "mascotas": "mascotas",
    "perro": "mascotas",
    "gato": "mascotas",
    "conservas": "conservas",
    "tomate": "conservas",
    "atun": "conservas",
    "sopa": "conservas",
    "caldo": "conservas",
    "arroz": "pasta",
    "pasta": "pasta",
    "legumbres": "pasta",
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
# HTTP
# ─────────────────────────────────────────────────────────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]
_ua_index = 0


def _siguiente_ua() -> str:
    global _ua_index
    ua = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
    _ua_index += 1
    return ua


def fetch_json(url: str, params: dict = None) -> Optional[dict]:
    """Descarga JSON desde la API con reintentos."""
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            headers = {"User-Agent": _siguiente_ua()}
            r = requests.get(url, params=params, headers=headers, timeout=30)
            
            if r.status_code == 429:
                time.sleep(BACKOFF_BASE ** intento)
                continue
            
            if r.status_code in (502, 503, 504):
                time.sleep(BACKOFF_BASE ** intento)
                continue
            
            if r.status_code == 404:
                log.warning(f"404 Not Found: {url}")
                return None
            
            r.raise_for_status()
            return r.json()
            
        except requests.exceptions.RequestException as e:
            log.warning(f"Error en {url} (intento {intento}): {e}")
            time.sleep(BACKOFF_BASE ** intento)
    
    log.error(f"No se pudo descargar {url} tras {MAX_REINTENTOS} intentos")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# API DE RADARSUPER
# ─────────────────────────────────────────────────────────────────────────────
def obtener_productos_cadena(cadena_slug: str, page: int = 1, limit: int = 100) -> Optional[dict]:
    """Obtiene productos de una cadena desde la API."""
    url = f"{BASE_URL}/products"
    params = {
        "chain": cadena_slug,
        "page": page,
        "limit": limit,
    }
    return fetch_json(url, params=params)


def obtener_categorias_cadena(cadena_slug: str) -> list[dict]:
    """Obtiene todas las categorías de una cadena desde la API."""
    url = f"{BASE_URL}/chains/{cadena_slug}/categories"
    data = fetch_json(url)
    
    if not data or "categories" not in data:
        return []
    
    categorias = []
    for cat in data.get("categories", []):
        slug = cat.get("slug", "")
        nombre = cat.get("name", "")
        
        # Mapear a categoría de la app
        cat_app = _mapear_categoria(slug, nombre)
        
        categorias.append({
            "slug": slug,
            "nombre": nombre,
            "cat_app": cat_app,
        })
    
    return categorias


def _mapear_categoria(slug: str, nombre: str) -> str:
    """Mapea un slug/nombre de categoría a la categoría de gastos_app."""
    texto = f"{slug} {nombre}".lower()
    
    for palabra_clave, categoria in CATEGORIA_MAP.items():
        if palabra_clave in texto:
            return categoria
    
    log.info(f"📋 Categoría sin mapear: {slug} → {nombre} (usando 'general')")
    return "general"


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


def cargar_productos(sb: Client, page_size: int = 1000) -> list[dict]:
    """Carga paginada de productos."""
    todos = []
    offset = 0
    while True:
        try:
            res = (
                sb.table("productos")
                .select("id, nombre")
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
    
    log.info(f"📦 {len(todos)} productos cargados")
    return todos


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


def insertar_producto(sb: Client, nombre: str, categoria: str, tienda_id: str) -> Optional[str]:
    for intento in range(1, 3):
        try:
            res = sb.table("productos").insert({
                "nombre": nombre,
                "categoria": categoria,
                "tienda_origen": tienda_id,
                "verificado": False
            }).execute()
            return res.data[0]["id"] if res.data else None
        except Exception as e:
            log.error(f"Error insertando producto (intento {intento}): {e}")
            time.sleep(1)
    return None


def upsert_precio(sb: Client, producto_id: str, tienda_id: str, precio: float):
    for intento in range(1, 3):
        try:
            sb.table("precios").upsert({
                "producto_id": producto_id,
                "tienda_id": tienda_id,
                "ciudad": CIUDAD_SCRAPER,
                "precio": precio,
                "fecha": str(date.today()),
                "fuente": "radarsuper"
            }, on_conflict="producto_id,tienda_id,ciudad,fecha").execute()
            return
        except Exception as e:
            log.error(f"Error upsert precio (intento {intento}): {e}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
def scrape_cadena(
    sb: Client,
    cadena_slug: str,
    cadena_nombre: str,
    tienda_id: str,
    productos_db: list[dict],
    stats: dict,
    modo_test: bool = False,
):
    """Scraper de una cadena completa."""
    log.info(f"\n🏪 Procesando {cadena_nombre} ({cadena_slug})...")
    
    # Obtener categorías
    categorias = obtener_categorias_cadena(cadena_slug)
    if not categorias:
        log.error(f"No se encontraron categorías para {cadena_nombre}")
        return
    
    log.info(f"  → {len(categorias)} categorías encontradas")
    
    if modo_test:
        categorias = categorias[:3]
        log.info(f"Modo test: {len(categorias)} categorías")
    
    # Procesar cada categoría
    for i, categoria in enumerate(categorias, 1):
        log.info(f"\n  [{i}/{len(categorias)}] {categoria['nombre']}")
        
        page = 1
        total_categoria = 0
        
        while True:
            # Obtener productos de esta categoría
            data = obtener_productos_cadena(cadena_slug, page=page)
            
            if not data or "products" not in data:
                break
            
            productos = data.get("products", [])
            if not productos:
                break
            
            log.info(f"     Página {page} → {len(productos)} productos")
            
            for item in productos:
                stats["procesados"] += 1
                total_categoria += 1
                
                nombre = item.get("name", "").strip()
                precio = item.get("price")
                
                if not nombre or precio is None:
                    stats["sin_precio"] += 1
                    continue
                
                try:
                    precio = float(precio)
                except (ValueError, TypeError):
                    stats["sin_precio"] += 1
                    continue
                
                # Fuzzy match
                producto_id, score = buscar_producto_fuzzy(nombre, productos_db)
                
                if producto_id:
                    stats["actualizados"] += 1
                    log.debug(f"Match ({score}%): '{nombre}'")
                else:
                    log.info(f"Nuevo producto: '{nombre}' (score: {score}%)")
                    producto_id = insertar_producto(sb, nombre, categoria["cat_app"], tienda_id)
                    if not producto_id:
                        stats["errores_db"] += 1
                        continue
                    productos_db.append({"id": producto_id, "nombre": nombre})
                    stats["nuevos"] += 1
                
                upsert_precio(sb, producto_id, tienda_id, precio)
                time.sleep(0.1)
            
            page += 1
            time.sleep(SLEEP_ENTRE_REQUESTS)
            
            # En modo test, solo 1 página por categoría
            if modo_test:
                break
        
        log.info(f"     Total categoría: {total_categoria} productos")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(cadena_seleccionada: Optional[str] = None, modo_test: bool = False):
    inicio_ts = datetime.now()
    log.info("=" * 70)
    log.info(f"🛒 Scraper RadarSuper v3 (API) — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info("=" * 70)

    try:
        sb = get_supabase()
        log.info("✅ Conectado a Supabase")
    except ValueError as e:
        log.error(f"❌ {e}")
        sys.exit(1)

    # Determinar cadenas a procesar
    if cadena_seleccionada and cadena_seleccionada != "todas":
        if cadena_seleccionada not in CADENAS_SOPORTADAS:
            log.error(f"❌ Cadena no soportada: {cadena_seleccionada}")
            sys.exit(1)
        cadenas = {cadena_seleccionada: CADENAS_SOPORTADAS[cadena_seleccionada]}
    else:
        cadenas = CADENAS_SOPORTADAS

    productos_globales = cargar_productos(sb)
    
    stats = {
        "procesados": 0,
        "actualizados": 0,
        "nuevos": 0,
        "sin_precio": 0,
        "errores_db": 0,
    }

    for cadena_slug, cadena_nombre in cadenas.items():
        tienda_id = cargar_tienda(sb, cadena_nombre)
        if not tienda_id:
            log.warning(f"⚠️ Tienda '{cadena_nombre}' no encontrada en Supabase")
            continue

        scrape_cadena(sb, cadena_slug, cadena_nombre, tienda_id, productos_globales, stats, modo_test)

    duracion = datetime.now() - inicio_ts
    log.info("=" * 70)
    log.info(f"✅ Scraper finalizado en {duracion}")
    log.info(f"   Productos procesados: {stats['procesados']}")
    log.info(f"   Precios actualizados: {stats['actualizados']}")
    log.info(f"   Productos nuevos: {stats['nuevos']}")
    log.info(f"   Sin precio: {stats['sin_precio']}")
    log.info(f"   Errores DB: {stats['errores_db']}")
    log.info("=" * 70)

    # Guardar resumen
    try:
        Path("scraper_radarsuper_resumen.json").write_text(
            json.dumps({"fecha": str(date.today()), "duracion": str(duracion), **stats}, indent=2)
        )
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper RadarSuper v3 (API Based)")
    parser.add_argument("--cadena", choices=list(CADENAS_SOPORTADAS.keys()) + ["todas"],
                        default="todas", help="Cadena a procesar")
    parser.add_argument("--test", action="store_true", help="Modo test")
    args = parser.parse_args()

    main(cadena_seleccionada=args.cadena, modo_test=args.test)
