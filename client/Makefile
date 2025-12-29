# Detect OS
UNAME := $(shell uname -s)

# Fetch version from pyproject.toml
VERSION := $(shell uv version --short)

APP_NAME := Keeper
GUI_ENTRY := src/acearchive_keeper/gui.py
CERTIFI := $(shell uv run python -c "import certifi; print(certifi.where())")

.PHONY: help

help:
	@echo ""
	@echo "Available high-level targets:"
	@echo ""
	@echo "  mac       Build the macOS .app bundle"
	@echo "  dmg       Build the macOS DMG installer"
	@echo "  release   Build the appropriate installer or package for this OS"
	@echo "  clean     Clean the build, dist, and artifacts dirs"
	@echo ""


# Directory targets
artifacts:
	mkdir artifacts

dist:
	mkdir dist

# macOS builds
dist/$(APP_NAME).app: $(SRC_FILES) | dist
	@if [ "$(UNAME)" != "Darwin" ]; then echo "macOS only"; exit 1; fi
	uv run pyinstaller --clean --noconfirm --name $(APP_NAME) --windowed --onedir \
	--add-data "$(CERTIFI):certifi" --icon assets/AceArchiveKeeper.icns $(GUI_ENTRY)

artifacts/$(APP_NAME)-$(VERSION).dmg: dist/$(APP_NAME).app clean-dmg | artifacts
	mkdir -p build/dmg
	cp -R dist/$(APP_NAME).app build/dmg/
	ln -s /Applications build/dmg/Applications

	hdiutil create \
		-volname "$(APP_NAME)" \
		-srcfolder build/dmg \
		-ov \
		-format UDZO \
		$@

# High-level targets
.PHONY: mac dmg release clean-dmg clean-build clean-dist clean-artifacts clean

mac: dist/$(APP_NAME).app
dmg: artifacts/$(APP_NAME)-$(VERSION).dmg

release:
	@if [ "$(UNAME)" = "Darwin" ]; then $(MAKE) dmg;
	else echo "Unsupported OS"; exit 1; fi

clean-dmg:
	rm -rf build/dmg

clean-build:
	rm -rf build

clean-dist:
	rm -rf dist

clean-artifacts:
	rm -rf artifacts

clean: clean-build clean-dist clean-artifacts
