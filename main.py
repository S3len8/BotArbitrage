"""
main.py — точка входа.

GUI запускается на http://localhost:8080
Торговля запускается кнопкой START в дашборде.
"""

import asyncio
import webbrowser
import threading
import uvicorn


def _open_browser():
    """Открывает браузер через 1.5 сек после запуска сервера."""
    import time
    time.sleep(1.5)
    webbrowser.open("http://localhost:8080")


async def run_gui():
    config = uvicorn.Config(
        "gui_server:app",
        host="127.0.0.1",   # localhost вместо 0.0.0.0 — работает на Windows
        port=8080,
        log_level="info",
        reload=True,
    )
    await uvicorn.Server(config).serve()


async def main():
    print("=" * 45)
    print("  ARB BOT")
    print("  Dashboard → http://localhost:8080")
    print("  Нажми START в дашборде чтобы начать")
    print("=" * 45)

    # Открываем браузер в отдельном потоке
    threading.Thread(target=_open_browser, daemon=True).start()

    await run_gui()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")