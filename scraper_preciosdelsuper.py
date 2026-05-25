"""
scraper_preciosdelsuper.py
==========================
Scraper diario de preciosdelsuper.es → Supabase (gastos_app)

MEJORAS DE ROBUSTEZ v3:
  - Timeouts agresivos en todas las peticiones
  - Sistema de heartbeat para evitar timeout de GitHub Actions
  - Reintentos con backoff exponencial mejorado
  - Pausas aleatorias entre peticiones
  - Guardado de progreso para reanudar
  - Timeout global por página (evita que se cuelgue)

PROTECCIÓN DE DATOS v4:
  - Respeta tickets de usuarios (no machaca precios de tickets recientes)
  - Los tickets de los últimos 7 días tienen prioridad
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
import random
import signal
from datetime import date, datetime, timedelta
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

# TIEMPOS (aumentados y aleatorios)
SLEEP_PAGINA_MIN = 2.0
SLEEP_PAGINA_MAX = 5.0
SLEEP_PRODUCTO_MIN = 0.3
SLEEP_PRODUCTO_MAX = 1.0
HEARTBEAT_INTERVAL = 50  # Cada 50 productos imprime un heartbeat

# TIMEOUTS (clave para que no se cuelgue)
REQUEST_TIMEOUT = 30           # Timeout global para cada petición
PAGINA_TIMEOUT = 60            # Timeout máximo por página

# Reintentos de red
MAX_REINTENTOS = 5
BACKOFF_BASE = 2.0

# PROTECCIÓN DE TICKETS: días que un ticket es considerado "reciente"
DIAS_TICKET_RECIENTE = 7

# Carpeta donde se guardan HTMLs de diagnóstico y progreso
DIAG_DIR = Path("diagnostico_scraper")
PROGRESS_FILE = Path("progreso_preciosdelsuper.json")

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO DE TIENDAS
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
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────
def random_sleep(min_sec: float, max_sec: float):
    """Pausa aleatoria para simular comportamiento humano."""
    time.sleep(random.uniform(min_sec, max_sec))


def heartbeat(contador: int):
    """Imprime un mensaje periódico para que GitHub Actions no mate el proceso."""
    if contador % HEARTBEAT_INTERVAL == 0 and contador > 0:
        log.info(f"❤️ Heartbeat: {contador} productos procesados...")


class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Timeout excedido")


def fetch_page_with_timeout(url: str, timeout_seconds: int = PAGINA_TIMEOUT):
    """Descarga una página con timeout global."""
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout_seconds)
    
    try:
        result = fetch_page(url)
        signal.alarm(0)
        return result
    except TimeoutError:
        log.error(f"⏰ TIMEOUT GLOBAL: La página {url} excedió {timeout_seconds}s")
        return None
    except Exception as e:
        signal.alarm(0)
        raise e


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — sesión con reintentos y backoff exponencial
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
    })
    return s


_sesion = _crear_sesion()


def fetch_page(url: str, diagnostico: bool = False, retry_count: int = 0) -> Optional[BeautifulSoup]:
    """Descarga una página con reintentos y backoff exponencial."""
    global _sesion

    for intento in range(1, MAX_REINTENTOS + 1):
        _sesion.headers["User-Agent"] = _siguiente_ua()
        try:
            r = _sesion.get(url, timeout=(5, REQUEST_TIMEOUT))

            if r.status_code == 429:
                espera = float(r.headers.get("Retry-After", BACKOFF_BASE ** intento))
                log.warning(f"429 Rate limit en {url} — esperando {espera:.0f}s")
                time.sleep(espera)
                continue

            if r.status_code in (503, 502, 504):
                espera = BACKOFF_BASE ** intento
                log.warning(f"HTTP {r.status_code} en {url} — reintento {intento}/{MAX_REINTENTOS} en {espera:.0f}s")
                time.sleep(espera)
                _sesion = _crear_sesion()
                continue

            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            if diagnostico or _parece_vacia(soup):
                _guardar_diagnostico(url, r.text)

            return soup

        except requests.exceptions.ConnectionError as e:
            espera = (BACKOFF_BASE ** intento) + random.uniform(0, 1)
            log.warning(f"Error de conexión en {url} (intento {intento}/{MAX_REINTENTOS}): {e}")
            if intento < MAX_REINTENTOS:
                log.info(f"Reintentando en {espera:.1f}s...")
                time.sleep(espera)
                _sesion = _crear_sesion()
            continue

        except requests.exceptions.Timeout:
            espera = (BACKOFF_BASE ** intento) + random.uniform(0, 1)
            log.warning(f"Timeout en {url} (intento {intento}/{MAX_REINTENTOS})")
            if intento < MAX_REINTENTOS:
                log.info(f"Reintentando en {espera:.1f}s...")
                time.sleep(espera)
            continue

        except requests.RequestException as e:
            log.error(f"Error irrecuperable en {url}: {e}")
            return None

    log.error(f"No se pudo descargar {url} tras {MAX_REINTENTOS} intentos")
    return None


def _parece_vacia(soup: BeautifulSoup) -> bool:
    """Detecta páginas sin contenido útil."""
    texto = soup.get_text(strip=True)
    if len(texto) < 200:
        return True
    alertas = ["acceso restringido", "captcha", "403", "503", "error interno", "cloudflare"]
    return any(a in texto.lower() for a in alertas)


def _guardar_diagnostico(url: str, html: str):
    """Guarda HTML para depuración."""
    try:
        DIAG_DIR.mkdir(exist_ok=True)
        nombre = hashlib.md5(url.encode()).hexdigest()[:8]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = DIAG_DIR / f"{ts}_{nombre}.html"
        path.write_text(html[:500000], encoding="utf-8")
        log.warning(f"🔍 HTML guardado en {path}")
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
    r'data-price=["\']?(\d{1,4}[.,]\d{2})',
]


def extraer_precio(texto: str) -> Optional[float]:
    """Extrae un precio del texto usando múltiples patrones."""
    texto_lower = texto.lower()
    for patron in _PATRONES_PRECIO:
        m = re.search(patron, texto_lower)
        if m:
            try:
                valor = float(m.group(1).replace(",", "."))
                if 0.05 <= valor <= 999.0:
                    return valor
            except ValueError:
                continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE NOMBRE
# ─────────────────────────────────────────────────────────────────────────────
_SELECTORES_NOMBRE = [
    ".product-name",
    ".product-title",
    "[class*='product'] h2",
    "[class*='product'] h3",
    "[class*='nombre']",
    "[class*='titulo']",
    "h2", "h3", "h4",
    "p:first-of-type",
]


def extraer_nombre(elemento: Tag) -> Optional[str]:
    """Extrae el nombre del producto usando múltiples selectores."""
    for selector in _SELECTORES_NOMBRE:
        el = elemento.select_one(selector)
        if el:
            texto = el.get_text(strip=True)
            if texto and len(texto) >= 3:
                return _limpiar_nombre(texto)

    texto = elemento.get_text(" ", strip=True)
    texto_limpio = re.sub(r"\d+[.,]\d+\s*€.*", "", texto).strip()
    if len(texto_limpio) >= 3:
        return _limpiar_nombre(texto_limpio[:120])

    return None


def _limpiar_nombre(nombre: str) -> str:
    """Normaliza el nombre."""
    nombre = re.sub(r"\s+", " ", nombre)
    nombre = re.sub(r"[^\w\sáéíóúàèìòùäëïöüñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑÇ.,\-%()/]", "", nombre)
    return nombre.strip()


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE TIENDA
# ─────────────────────────────────────────────────────────────────────────────
_SELECTORES_TIENDA = [
    "img[alt]",
    "[class*='tienda'] img",
    "[class*='supermercado'] img",
    "[class*='logo'] img",
]


def extraer_tienda_raw(elemento: Tag) -> str:
    """Extrae el nombre crudo de la tienda."""
    for selector in _SELECTORES_TIENDA:
        img = elemento.select_one(selector)
        if img:
            alt = img.get("alt", "").strip()
            if alt and len(alt) >= 2:
                return alt.upper()
            src = img.get("src", "")
            nombre_archivo = Path(src).stem.upper()
            if nombre_archivo:
                return nombre_archivo
    return ""


def resolver_tienda(tienda_raw: str, tiendas_map: dict[str, str]) -> Optional[str]:
    """Convierte el nombre crudo al ID de tienda."""
    if not tienda_raw:
        return None

    if tienda_raw in TIENDA_MAP:
        nombre_norm = TIENDA_MAP[tienda_raw]
        return tiendas_map.get(nombre_norm)

    for clave, nombre_norm in TIENDA_MAP.items():
        if clave in tienda_raw or tienda_raw in clave:
            return tiendas_map.get(nombre_norm)

    mejor_score = 0
    mejor_id = None
    for nombre_norm, tid in tiendas_map.items():
        score = fuzz.ratio(tienda_raw.lower(), nombre_norm.lower())
        if score > mejor_score and score >= 80:
            mejor_score = score
            mejor_id = tid

    return mejor_id


# ─────────────────────────────────────────────────────────────────────────────
# PAGINACIÓN
# ─────────────────────────────────────────────────────────────────────────────
def get_total_paginas(soup: BeautifulSoup) -> int:
    """Detecta el número total de páginas."""
    max_page = 0
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))

    if max_page > 0:
        return max_page

    texto = soup.get_text()
    m = re.search(r"[Pp]ágina\s+\d+\s+[Dd]e\s+(\d+)", texto)
    if m:
        return int(m.group(1))

    return 1


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS
# ─────────────────────────────────────────────────────────────────────────────
_SELECTORES_CARD = [
    "a[href*='/producto/']",
    "a[href*='/product/']",
    "[class*='product-card']",
    "[class*='producto-card']",
    "article",
]


def parsear_pagina(soup: BeautifulSoup, diagnostico: bool = False) -> list[dict]:
    """Extrae los productos de una página."""
    productos: list[dict] = []
    vistos: set[str] = set()

    for selector in _SELECTORES_CARD:
        elementos = soup.select(selector)
        if not elementos:
            continue

        for elemento in elementos:
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
            url_producto = BASE_URL + href if href and href.startswith("/") else href

            productos.append({
                "nombre": nombre,
                "precio": precio,
                "tienda_raw": tienda_raw,
                "url": url_producto,
            })

        if productos:
            break

    return productos


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE - PROTECCIÓN DE TICKETS
# ─────────────────────────────────────────────────────────────────────────────
_sb_client = None

def get_supabase() -> Client:
    global _sb_client
    if _sb_client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Faltan SUPABASE_URL o SUPABASE_KEY en .env")
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb_client


def cargar_tiendas(sb: Client) -> dict[str, str]:
    try:
        res = sb.table("tiendas").select("id, nombre").execute()
        return {r["nombre"]: r["id"] for r in res.data}
    except Exception as e:
        log.error(f"Error cargando tiendas: {e}")
        return {}


def cargar_productos(sb: Client, page_size: int = 1000) -> list[dict]:
    todos = []
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
            log.error(f"Error cargando productos: {e}")
            break
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


def insertar_producto(sb: Client, nombre: str, tienda_id: str) -> Optional[str]:
    try:
        res = sb.table("productos").insert({
            "nombre": nombre,
            "tienda_origen": tienda_id,
            "verificado": False,
        }).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.error(f"Error insertando producto: {e}")
        return None


def buscar_producto_id(sb: Client, nombre: str) -> Optional[str]:
    """Busca el ID de un producto por nombre exacto."""
    try:
        res = sb.table("productos").select("id").eq("nombre", nombre).limit(1).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.warning(f"Error buscando producto: {e}")
        return None


def buscar_tienda_id(sb: Client, nombre: str) -> Optional[str]:
    """Busca el ID de una tienda por nombre."""
    try:
        res = sb.table("tiendas").select("id").eq("nombre", nombre).limit(1).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.warning(f"Error buscando tienda: {e}")
        return None


def tiene_ticket_reciente(sb: Client, producto_id: str, tienda_id: str, ciudad: str) -> bool:
    """Verifica si existe un precio de tipo 'ticket' en los últimos DIAS_TICKET_RECIENTE días."""
    try:
        fecha_limite = (datetime.now() - timedelta(days=DIAS_TICKET_RECIENTE)).date().isoformat()
        
        result = sb.table('precies')\
            .select('id')\
            .eq('producto_id', producto_id)\
            .eq('tienda_id', tienda_id)\
            .eq('fuente', 'ticket')\
            .gte('fecha', fecha_limite)\
            .limit(1)\
            .execute()
        
        return len(result.data) > 0
    except Exception as e:
        log.warning(f"⚠️ Error verificando ticket reciente: {e}")
        return False


def upsert_precio_protegido(sb: Client, producto_id: str, tienda_id: str, precio: float, ciudad: str, fuente: str = 'preciosdelsuper') -> bool:
    """Inserta o actualiza el precio, respetando tickets recientes."""
    
    # Verificar si hay un ticket reciente
    if tiene_ticket_reciente(sb, producto_id, tienda_id, ciudad):
        log.info(f"⚠️ Saltando actualización: existe ticket reciente (últimos {DIAS_TICKET_RECIENTE} días)")
        return False
    
    try:
        sb.table("precios").upsert({
            "producto_id": producto_id,
            "tienda_id": tienda_id,
            "ciudad": ciudad,
            "precio": precio,
            "fecha": str(date.today()),
            "fuente": fuente,
        }, on_conflict="producto_id,tienda_id,ciudad,fecha").execute()
        log.info(f"✅ Precio actualizado: {precio}€ para producto {producto_id}")
        return True
    except Exception as e:
        log.error(f"Error upsert precio: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# GUARDADO DE PROGRESO
# ─────────────────────────────────────────────────────────────────────────────
def guardar_progreso(page_num: int, stats: dict):
    """Guarda el progreso para poder reanudar si falla."""
    try:
        progreso = {
            "ultima_pagina": page_num,
            "stats": stats,
            "fecha": str(date.today())
        }
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progreso, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.debug(f"No se pudo guardar progreso: {e}")


def cargar_progreso() -> tuple[int, dict]:
    """Carga el progreso guardado."""
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progreso = json.load(f)
            return progreso.get("ultima_pagina", 1), progreso.get("stats", {})
    except Exception:
        pass
    return 1, {}


def limpiar_progreso():
    """Elimina el archivo de progreso al finalizar."""
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(page_inicio: int = 1, page_fin: Optional[int] = None, diagnostico: bool = False):
    inicio_ts = datetime.now()
    log.info("=" * 65)
    log.info(f"🛒 Scraper preciosdelsuper.es v4 — inicio: {inicio_ts:%Y-%m-%d %H:%M:%S}")
    log.info(f"🔒 Protección de tickets: últimos {DIAS_TICKET_RECIENTE} días")
    log.info("=" * 65)

    # Conectar a Supabase
    sb = get_supabase()
    log.info("✅ Conectado a Supabase")

    tiendas_map = cargar_tiendas(sb)
    productos_db = cargar_productos(sb)

    if not tiendas_map:
        log.error("❌ No se pudieron cargar las tiendas. Abortando.")
        sys.exit(1)

    # Detectar total de páginas
    log.info("📡 Detectando número total de páginas...")
    primera_soup = fetch_page(CAMBIOS_URL, diagnostico=diagnostico)
    if not primera_soup:
        log.error("❌ No se pudo acceder a preciosdelsuper.es. Abortando.")
        sys.exit(1)

    total_paginas = get_total_paginas(primera_soup)
    if page_fin is None or page_fin > total_paginas:
        page_fin = total_paginas

    log.info(f"📄 Procesando páginas {page_inicio} → {page_fin} de {total_paginas}")

    # Cargar progreso previo
    ultima_pagina, stats_prev = cargar_progreso()
    if ultima_pagina > page_inicio:
        log.info(f"🔄 Reanudando desde página {ultima_pagina}")
        page_inicio = ultima_pagina
    else:
        stats_prev = {}

    stats = {
        "procesados": stats_prev.get("procesados", 0),
        "actualizados": stats_prev.get("actualizados", 0),
        "nuevos_prod": stats_prev.get("nuevos_prod", 0),
        "sin_tienda": stats_prev.get("sin_tienda", 0),
        "sin_precio": stats_prev.get("sin_precio", 0),
        "errores_db": stats_prev.get("errores_db", 0),
        "paginas_vacias": stats_prev.get("paginas_vacias", 0),
        "saltados_ticket": stats_prev.get("saltados_ticket", 0),
    }

    # Bucle principal
    for page_num in range(page_inicio, page_fin + 1):
        url = f"{CAMBIOS_URL}?page={page_num}" if page_num > 1 else CAMBIOS_URL
        log.info(f"━━ Página {page_num}/{page_fin} ━━")

        soup = fetch_page_with_timeout(url, PAGINA_TIMEOUT)
        if not soup:
            log.warning(f"Saltando página {page_num}")
            stats["paginas_vacias"] += 1
            guardar_progreso(page_num + 1, stats)
            continue

        items = parsear_pagina(soup, diagnostico=diagnostico)
        log.info(f"   → {len(items)} productos extraídos")

        for idx, item in enumerate(items):
            stats["procesados"] += 1
            heartbeat(stats["procesados"])

            if not item["nombre"] or item["precio"] is None:
                stats["sin_precio"] += 1
                continue

            tienda_id = resolver_tienda(item["tienda_raw"], tiendas_map)
            if not tienda_id:
                stats["sin_tienda"] += 1
                continue

            match, score = buscar_producto_fuzzy(item["nombre"], productos_db)

            if match:
                producto_id = match["id"]
                stats["actualizados"] += 1
            else:
                log.info(f"Nuevo: '{item['nombre'][:40]}'")
                producto_id = insertar_producto(sb, item["nombre"], tienda_id)
                if not producto_id:
                    stats["errores_db"] += 1
                    continue
                productos_db.append({
                    "id": producto_id,
                    "nombre": item["nombre"],
                    "marca": None,
                    "tienda_origen": tienda_id,
                })
                stats["nuevos_prod"] += 1

            # Subir precio con protección de tickets
            exito = upsert_precio_protegido(sb, producto_id, tienda_id, item["precio"], CIUDAD_SCRAPER, 'preciosdelsuper')
            if not exito:
                stats["saltados_ticket"] += 1
            
            random_sleep(SLEEP_PRODUCTO_MIN, SLEEP_PRODUCTO_MAX)

        # Guardar progreso después de cada página
        guardar_progreso(page_num + 1, stats)
        random_sleep(SLEEP_PAGINA_MIN, SLEEP_PAGINA_MAX)

    # Limpiar progreso al finalizar
    limpiar_progreso()

    duracion = datetime.now() - inicio_ts
    log.info("=" * 65)
    log.info(f"✅ Scraper finalizado en {duracion}")
    log.info(f"   Productos procesados: {stats['procesados']}")
    log.info(f"   Precios actualizados: {stats['actualizados']}")
    log.info(f"   Productos nuevos: {stats['nuevos_prod']}")
    log.info(f"   Saltados por ticket reciente: {stats['saltados_ticket']}")
    log.info("=" * 65)

    # Guardar resumen
    try:
        resumen = {
            "fecha": str(date.today()),
            "duracion": str(duracion),
            **stats
        }
        Path("scraper_resumen.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False))
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper preciosdelsuper.es")
    parser.add_argument("--pages", nargs=2, type=int, metavar=("INICIO", "FIN"), help="Rango de páginas")
    parser.add_argument("--diagnostico", action="store_true", help="Guardar HTML para depuración")
    args = parser.parse_args()

    inicio = args.pages[0] if args.pages else 1
    fin = args.pages[1] if args.pages else None

    main(page_inicio=inicio, page_fin=fin, diagnostico=args.diagnostico)
