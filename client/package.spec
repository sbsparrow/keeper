# -*- mode': python ; coding': utf-8 -*-
import argparse
import os
import certifi


def semver_str(verstring: str) -> str:
    from semver.version import Version
    if Version.is_valid(verstring):
        return verstring
    else:
        raise argparse.ArgumentTypeError(f"{verstring} is not a valid semantic version.")


parser = argparse.ArgumentParser()
parser.add_argument("--version", type=semver_str, required=False)
parser.add_argument('--platform', choices=['Darwin', 'Linux', 'Windows_NT'],
                    help='The platform to build on', required=True)
args = parser.parse_args()
version = args.version
platform = args.platform

if platform == 'Darwin':
    icon = os.path.join("build", "keeper.icns")
elif platform == "Windows_NT":
    icon = os.path.join("assets", "keeper.ico")
else:
    icon = None

analysis_kwargs = {
    'pathex': [],
    'binaries': [],
    'datas': [(certifi.where(), 'certifi'), ("assets/keeper_32x32.png", "assets")],
    'hiddenimports': [],
    'hookspath': [],
    'hooksconfig': {},
    'runtime_hooks': [],
    'excludes': [],
    'noarchive': False,
    'optimize': 0
}

exe_kwargs = {
    'exclude_binaries': True,
    'debug': False,
    'bootloader_ignore_signals': False,
    'strip': False,
    'upx': True,
    'disable_windowed_traceback': False,
    'argv_emulation': False,
    'target_arch': None,
    'codesign_identity': None,
    'entitlements_file': None
}

cli_a = Analysis(
    [os.path.join("src", "acearchive_keeper", "__init__.py")],
    **analysis_kwargs
)
gui_a = Analysis(
    [os.path.join("src", "acearchive_keeper", "gui.py")],
    **analysis_kwargs
)

cli_pyz = PYZ(cli_a.pure)
gui_pyz = PYZ(gui_a.pure)

cli_exe = EXE(
    cli_pyz,
    cli_a.scripts,
    [],
    name='keeper-cli',
    console=True,
    **exe_kwargs
)
gui_exe = EXE(
    gui_pyz,
    gui_a.scripts,
    [],
    name='Keeper',
    console=False,
    icon=[icon],
    **exe_kwargs
)

if platform == 'Darwin':
    app = BUNDLE(
        gui_exe,
        gui_a.binaries,
        gui_a.datas,
        cli_exe,
        cli_a.binaries,
        cli_a.datas,
        strip=False,
        upx=True,
        upx_exclude=[],
        name='Keeper.app',
        icon=icon,
        version=version,
        bundle_identifier=None,
        info_plist={'NSPrincipalClass': 'NSApplication'}
    )
