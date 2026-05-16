"""
scraper_radarsuper_v2.py - Scraper en dos fases
Fase 1: Extrae todos los enlaces de productos de las categorías
Fase 2: Scrapea cada producto individualmente
"""

import re
import time
import json
from typing import Optional, List
from bs4 import BeautifulSoup

# ... (mantén las funciones fetch, extraer_categorias, etc. del script anterior)

def extraer_enlaces_productos_de_categoria(soup: BeautifulSoup, cadena_slug: str) -> List[str]:
    """Extrae todos los enlaces de productos de una página de categoría."""
    enlaces = []
    patron = re.compile(rf"/{cadena_slug}/p/[\w-]+")
    
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if patron.search(href):
            url_completa = BASE_URL + href if href.startswith("/") else href
            if url_completa not in enlaces:
                enlaces.append(url_completa)
    
    return enlaces


def scrapear_producto_desde_url(url: str, diagnostico: bool = False) -> Optional[dict]:
    """Extrae datos de un producto desde su URL individual."""
    soup = fetch(url, diagnostico=diagnostico)
    if not soup:
        return None
    
    try:
        # Nombre del producto
        nombre_elem = soup.find("h1")
        nombre = nombre_elem.get_text(strip=True) if nombre_elem else None
        
        # Precio principal - buscar patrón cerca del h1
        precio = None
        if nombre_elem:
            precio_texto = nombre_elem.find_next(string=re.compile(r"\d+[.,]\d+\s*€"))
            if precio_texto:
                precio = extraer_precio(precio_texto)
        
        if not precio:
            # Buscar en toda la página
            texto = soup.get_text()
            precio = extraer_precio(texto)
        
        # Precio por kg/L
        precio_kg, unidad = None, None
        kg_texto = soup.find(string=re.compile(r"\d+[.,]\d+\s*€\s*/\s*(kg|L|Kg|l)"))
        if kg_texto:
            precio_kg, unidad = extraer_precio_kg(kg_texto)
        
        # Formato/peso
        formato = None
        formato_elem = soup.find(string=re.compile(r"Formato:|Peso:|Contenido:"))
        if formato_elem:
            formato = formato_elem.strip()
        
        return {
            "nombre": nombre,
            "precio": precio,
            "precio_kg": precio_kg,
            "unidad_precio": unidad,
            "formato": formato,
            "url": url
        }
        
    except Exception as e:
        log.error(f"Error scrapeando producto {url}: {e}")
        return None


def fase1_extraer_todos_los_enlaces(cadena_slug: str, diagnostico: bool = False) -> List[str]:
    """Fase 1: Extrae todos los enlaces de productos de todas las categorías."""
    todos_los_enlaces = []
    
    # Obtener categorías
    categorias = extraer_categorias(cadena_slug, diagnostico=diagnostico)
    log.info(f"📂 Procesando {len(categorias)} categorías para extraer enlaces...")
    
    for i, categoria in enumerate(categorias, 1):
        log.info(f"  [{i}/{len(categorias)}] {categoria['nombre']}")
        
        soup = fetch(categoria["url"], diagnostico=diagnostico)
        if not soup:
            continue
        
        # Extraer enlaces de esta categoría
        enlaces = extraer_enlaces_productos_de_categoria(soup, cadena_slug)
        log.info(f"      → {len(enlaces)} enlaces de productos encontrados")
        
        todos_los_enlaces.extend(enlaces)
        
        # Respetar límites de velocidad
        time.sleep(SLEEP_CATEGORIA)
    
    # Eliminar duplicados
    todos_los_enlaces = list(set(todos_los_enlaces))
    log.info(f"✅ Total de enlaces únicos encontrados: {len(todos_los_enlaces)}")
    
    # Guardar enlaces en un archivo
    enlaces_file = f"enlaces_{cadena_slug}.json"
    with open(enlaces_file, "w", encoding="utf-8") as f:
        json.dump(todos_los_enlaces, f, indent=2, ensure_ascii=False)
    log.info(f"💾 Enlaces guardados en {enlaces_file}")
    
    return todos_los_enlaces


def fase2_scrapear_productos(cadena_slug: str, diagnostico: bool = False, limite: int = None):
    """Fase 2: Scrapea cada producto individualmente desde los enlaces guardados."""
    enlaces_file = f"enlaces_{cadena_slug}.json"
    
    try:
        with open(enlaces_file, "r", encoding="utf-8") as f:
            enlaces = json.load(f)
    except FileNotFoundError:
        log.error(f"No se encuentra {enlaces_file}. Ejecuta la fase 1 primero.")
        return
    
    if limite:
        enlaces = enlaces[:limite]
        log.info(f"🧪 Modo test: procesando {len(enlaces)} productos")
    
    log.info(f"🔄 Scrapeando {len(enlaces)} productos...")
    
    productos_scrapeados = []
    
    for i, url in enumerate(enlaces, 1):
        log.info(f"  [{i}/{len(enlaces)}] {url}")
        
        producto = scrapear_producto_desde_url(url, diagnostico=diagnostico)
        if producto:
            productos_scrapeados.append(producto)
            log.info(f"      ✅ {producto['nombre']} - {producto['precio']}€")
        else:
            log.warning(f"      ❌ No se pudo scrapear")
        
        # Guardar progreso cada 10 productos
        if i % 10 == 0:
            with open(f"productos_{cadena_slug}_parcial.json", "w", encoding="utf-8") as f:
                json.dump(productos_scrapeados, f, indent=2, ensure_ascii=False)
        
        time.sleep(SLEEP_PRODUCTO)
    
    # Guardar resultados finales
    output_file = f"productos_{cadena_slug}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(productos_scrapeados, f, indent=2, ensure_ascii=False)
    
    log.info(f"✅ Scraping completado. {len(productos_scrapeados)} productos guardados en {output_file}")
    
    return productos_scrapeados


def main():
    parser = argparse.ArgumentParser(description="Scraper RadarSuper en dos fases")
    parser.add_argument("--cadena", default="mercadona", help="Cadena a procesar")
    parser.add_argument("--fase", choices=["1", "2", "ambas"], default="ambas", 
                        help="Fase a ejecutar: 1 (extraer enlaces), 2 (scrapear productos)")
    parser.add_argument("--test", action="store_true", help="Modo test (limitar productos)")
    parser.add_argument("--diagnostico", action="store_true", help="Guardar HTML")
    args = parser.parse_args()
    
    if args.fase in ["1", "ambas"]:
        enlaces = fase1_extraer_todos_los_enlaces(args.cadena, diagnostico=args.diagnostico)
    
    if args.fase in ["2", "ambas"]:
        limite = 10 if args.test else None
        productos = fase2_scrapear_productos(args.cadena, diagnostico=args.diagnostico, limite=limite)


if __name__ == "__main__":
    main()
