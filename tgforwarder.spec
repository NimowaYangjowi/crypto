# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['menubar_app.py'],
    pathex=['openclaw_trader'],
    binaries=[],
    datas=[
        ('templates', 'templates'),
    ],
    hiddenimports=[
        # Menu bar
        'rumps',
        'objc',
        'Foundation',
        'AppKit',
        'CoreFoundation',
        'PyObjCTools',
        'PyObjCTools.Conversion',
        'PyObjCTools.AppHelper',
        # Telegram
        'telethon',
        'telethon.crypto',
        'telethon.tl',
        'telethon.tl.types',
        'telethon.tl.functions',
        # Trading
        'ccxt',
        'ccxt.binance',
        'httpx',
        'httpx._transports',
        'httpx._transports.default',
        # Web server
        'aiohttp',
        'aiohttp.web',
        # Utilities
        'dotenv',
        'sqlite3',
        'pyaes',
        'rsa',
        # WebSocket
        'websockets',
        # App modules
        'app',
        'core',
        'core.config',
        'core.database',
        'core.auth',
        'forwarder',
        'forwarder.module',
        'signal_trader',
        'signal_trader.module',
        'signal_trader.parser',
        'signal_trader.exchange_sync',
        'bot_controller',
        'bot_controller.controller',
        'openclaw_trader',
        'openclaw_trader.bridge',
        'openclaw_trader.shared_settings',
        'openclaw_trader.trade',
        'openclaw_trader.watcher',
        'openclaw_trader.monitor',
        'dashboard',
        'dashboard.server',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
        'cv2',
        'test',
        'unittest',
        'pkg_resources',
        'setuptools',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='TGForwarder',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name='TGForwarder',
)

app = BUNDLE(
    coll,
    name='TG Forwarder.app',
    icon='icon.icns',
    bundle_identifier='com.tgforwarder.app',
    info_plist={
        'CFBundleName': 'TG Forwarder',
        'CFBundleDisplayName': 'TG Forwarder',
        'CFBundleVersion': '3.0.0',
        'CFBundleShortVersionString': '3.0.0',
        'LSMinimumSystemVersion': '10.15',
        'NSHighResolutionCapable': True,
        'LSUIElement': False,
    },
)
