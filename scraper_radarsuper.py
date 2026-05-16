# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE CATEGORÍAS Y SUBCATEGORÍAS
# ─────────────────────────────────────────────────────────────────────────────
def extraer_categorias(cadena_slug: str, diagnostico: bool = False) -> list[dict]:
    """
    Extrae automáticamente categorías y subcategorías navegando en profundidad.
    """
    url_principal = f"{BASE_URL}/{cadena_slug}"
    soup = fetch(url_principal, diagnostico=diagnostico)
    
    if not soup:
        log.error(f"No se pudo acceder a {url_principal}")
        return []
    
    categorias_principales = []
    vistos = set()
    
    # 1. Encontrar categorías de primer nivel (/cadena/c/nombre-id)
    patron_cat = re.compile(rf"/{cadena_slug}/c/([\w-]+)-(\d+)")
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        match = patron_cat.search(href)
        
        if match and href not in vistos:
            vistos.add(href)
            texto = a.get_text(strip=True)
            texto_limpio = re.sub(r"\s*\(\d+\)\s*$", "", texto).strip()
            
            if texto_limpio:
                url_completa = BASE_URL + href if href.startswith("/") else href
                categorias_principales.append({
                    "slug": match.group(1),
                    "nombre": texto_limpio,
                    "url": url_completa,
                    "es_subcategoria": False
                })

    # 2. Buscar subcategorías dentro de cada categoría principal
    categorias_finales = []
    # Patrón para enlaces que parecen productos o subcategorías
    patron_subcat = re.compile(rf"/{cadena_slug}/(?:c|p)/([\w-]+)(?:-\d+)?")
    
    log.info(f"🔍 Detectadas {len(categorias_principales)} categorías raíz. Buscando subcategorías...")

    for cat in categorias_principales:  # CORREGIDO: categorias_principales, no categories_principales
        log.info(f"   Explorando subcategorías de: {cat['nombre']}")
        soup_cat = fetch(cat["url"], diagnostico=diagnostico)
        time.sleep(SLEEP_PRODUCTO)
        
        if not soup_cat:
            continue
            
        subcats_encontradas = False
        
        # Buscar enlaces dentro de la página de categoría
        for a in soup_cat.find_all("a", href=True):
            href = a["href"]
            
            # Evitar duplicados
            if href in vistos:
                continue
            
            # Buscar patrones de subcategoría o producto
            match_sub = patron_subcat.search(href)
            
            if match_sub and "/c/" in href:  # Es una subcategoría
                vistos.add(href)
                texto_sub = a.get_text(strip=True)
                texto_sub_limpio = re.sub(r"\s*\(\d+\)\s*$", "", texto_sub).strip()
                
                if texto_sub_limpio and len(texto_sub_limpio) > 3:
                    url_sub = BASE_URL + href if href.startswith("/") else href
                    slug_sub = match_sub.group(1)
                    
                    # Heredar o mapear categoría de la app
                    cat_app = CATEGORIA_MAP.get(cat["slug"], CATEGORIA_MAP.get(slug_sub, "general"))
                    
                    categorias_finales.append({
                        "slug": slug_sub,
                        "nombre": f"{cat['nombre']} > {texto_sub_limpio}",
                        "url": url_sub,
                        "cat_app": cat_app,
                    })
                    subcats_encontradas = True
        
        # Si la categoría no tenía subcategorías, ella misma es la que contiene los productos
        if not subcats_encontradas:
            cat_app = CATEGORIA_MAP.get(cat["slug"], "general")
            cat["cat_app"] = cat_app
            categorias_finales.append(cat)

    log.info(f"  → Total de {len(categorias_finales)} listados finales listos para scrapear.")
    return sorted(categorias_finales, key=lambda x: x["nombre"])


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE PRODUCTOS (Versión mejorada)
# ─────────────────────────────────────────────────────────────────────────────
def parsear_productos_pagina(soup: BeautifulSoup, diagnostico: bool = False) -> list[dict]:
    """Extrae productos individuales asegurando que tengan estructura de producto real."""
    productos = []
    vistos = set()
    
    # ESTRATEGIA 1: Buscar contenedores de producto con clases comunes
    contenedores = soup.find_all(["div", "article"], class_=re.compile(r"(product|item|card|producto)", re.I))
    
    for contenedor in contenedores:
        # Buscar enlace dentro del contenedor
        enlace = contenedor.find("a", href=re.compile(r"/p/|/producto/|/articulo/"))
        if not enlace:
            continue
        
        href = enlace["href"]
        if href in vistos:
            continue
        
        # Extraer nombre
        nombre = None
        titulo_elem = contenedor.find(["h2", "h3", "h4", "span"], 
                                       class_=re.compile(r"(name|title|nombre)", re.I))
        if titulo_elem:
            nombre = titulo_elem.get_text(strip=True)
        
        if not nombre:
            nombre = enlace.get_text(strip=True)
        
        if not nombre or len(nombre) < 4:
            continue
        
        # Limpiar nombre
        nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", nombre).strip()
        nombre = re.sub(r"Ver producto|Comprar|Más información", "", nombre, flags=re.I).strip()
        
        # Extraer precio
        texto_completo = contenedor.get_text(" ", strip=True)
        precio = extraer_precio(texto_completo)
        
        if precio is None:
            # Buscar elemento específico de precio
            precio_elem = contenedor.find(class_=re.compile(r"(price|precio|cost)", re.I))
            if precio_elem:
                precio = extraer_precio(precio_elem.get_text(" ", strip=True))
        
        if precio is None:
            continue
        
        vistos.add(href)
        precio_kg, unidad = extraer_precio_kg(texto_completo)
        url_producto = BASE_URL + href if href.startswith("/") else href
        
        productos.append({
            "nombre": nombre[:150],
            "precio": precio,
            "precio_kg": precio_kg,
            "unidad_precio": unidad,
            "url": url_producto,
        })
    
    # ESTRATEGIA 2: Si no se encontraron productos, buscar directamente en enlaces
    if not productos:
        patron_producto = re.compile(r"/p/[\w-]+(?:-\d+)?")
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not patron_producto.search(href) or href in vistos:
                continue
            
            texto = a.get_text(" ", strip=True)
            
            # Verificar que tenga precio
            precio = extraer_precio(texto)
            if precio is None:
                padre = a.find_parent(["div", "article", "li"])
                if padre:
                    precio = extraer_precio(padre.get_text(" ", strip=True))
            
            if precio is None:
                continue
            
            # Extraer nombre
            nombre = re.sub(r"\d+[.,]\d+\s*€.*$", "", texto).strip()
            if not nombre or len(nombre) < 4:
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
    
    if not productos and diagnostico:
        _guardar_diagnostico("pagina_sin_productos", str(soup))
        log.warning("⚠️ No se encontraron productos - la estructura puede haber cambiado")
    
    return productos
