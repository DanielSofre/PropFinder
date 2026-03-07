"""
debug_scraper.py
Abre una página de Zonaprop y vuelca información diagnóstica.
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json, pathlib, time

URL = "https://www.zonaprop.com.ar/departamentos-venta-caballito.html"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="es-AR",
    )
    page = context.new_page()
    print(f"Cargando {URL} ...")
    page.goto(URL, wait_until="domcontentloaded", timeout=30_000)
    time.sleep(4)   # esperar JS extra

    html = page.content()
    # Guardar HTML completo para inspección
    pathlib.Path("debug_output.html").write_text(html, encoding="utf-8")
    print("HTML guardado en debug_output.html")

    soup = BeautifulSoup(html, "lxml")

    print(f"\n--- Título ---")
    print(soup.title.string if soup.title else "(sin título)")

    print(f"\n--- __NEXT_DATA__ ---")
    nd = soup.find("script", id="__NEXT_DATA__")
    print(f"Presente: {bool(nd)}")
    if nd and nd.string:
        try:
            data = json.loads(nd.string)
            pp = data.get("props", {}).get("pageProps", {})
            print("Keys en pageProps:", list(pp.keys())[:20])
            # Buscar listas de propiedades en cualquier key
            for k, v in pp.items():
                if isinstance(v, list) and len(v) > 0:
                    print(f"  Lista '{k}': {len(v)} items, primer item keys: {list(v[0].keys())[:10] if isinstance(v[0], dict) else type(v[0])}")
        except Exception as e:
            print(f"Error parseando JSON: {e}")

    print(f"\n--- Selectores CSS ---")
    for sel in [
        "[data-qa='POSTING_CARD']",
        ".postingCard",
        ".posting-card-layout",
        "[class*='posting']",
        "[class*='Posting']",
        "[class*='card']",
        "article",
        "li[class]",
    ]:
        count = len(soup.select(sel))
        if count:
            print(f"  ✅ {sel!r:45s} → {count}")
        else:
            print(f"  ❌ {sel!r:45s} → 0")

    browser.close()
    print("\nListo. Revisá debug_output.html para ver el HTML completo.")

