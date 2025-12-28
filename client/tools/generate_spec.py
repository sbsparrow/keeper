import os

import toml
from pathlib import Path

# Load pyproject.toml
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
with open(os.path.join(project_root, "pyproject.toml"), 'r') as fh:
    pyproject = toml.load(fh)

app_cfg = pyproject["tool"]["pyinstaller"]
app_name = app_cfg["name"]

# Entry script (absolute path)
entry_script = Path(app_cfg["entry"])
entry_script_abs = os.path.join(project_root, entry_script)

# Icons (absolute paths)
icon = app_cfg.get("icon")
icon_abs = os.path.join(project_root, icon) if icon else None

bundle_id = app_cfg.get("bundle_identifier", f"com.{app_name.lower()}")

build_dir = os.path.join(project_root, "build")

spec_content = f"""
# -*- mode: python ; coding: utf-8 -*-

import sys

a = Analysis(
    ["{entry_script_abs}"],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

# Required: build Python archive
pyz = PYZ(a.pure)

# Correct EXE signature for PyInstaller 6.x
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    name="{app_name}",
    icon="{icon_abs}",
    console=False,
)

# macOS bundle
app = BUNDLE(
    exe,
    name="{app_name}.app",
    icon="{icon_abs}",
    bundle_identifier="{bundle_id}",
)
"""

spec_file = os.path.join(build_dir, f"{app_name}.spec")
with open(spec_file, 'w') as fh:
    fh.write(spec_content)

print(f"Generated pyinstaller spec: {spec_file}")
