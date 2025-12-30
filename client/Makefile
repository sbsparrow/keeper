# Detect OS
ifeq ($(OS),Windows_NT)
	PLATFORM := $(OS)
	BIN_EXT := .exe
else
	PLATFORM := $(shell uname -s)
endif


# Fetch version from pyproject.toml
VERSION := $(shell uv version --short)

APP_NAME := Keeper
CLI_ENTRY := src/acearchive_keeper/__init__.py
GUI_ENTRY := src/acearchive_keeper/gui.py
SRC_FILES := $(shell git ls-files src)
CERTIFI := $(shell uv run python -c "import certifi; print(certifi.where())")

.PHONY: help

help:
	@echo ""
	@echo "Available high-level targets:"
	@echo ""
	@echo "  mac            Build the macOS .app bundle"
	@echo "  dmg            Build the macOS DMG installer"
	@echo "  bins           Build the binaries for the current OS"
	@echo "  zip            Bundle the binaries for the current OS into a zip"
	@echo "  package        Build the package for the current OS"
	@echo "  release        Build everything and copy to releases/"
	@echo "  clean          Clean build directories"
	@echo "  clean-releases Clean the releases directory"
	@echo "  clean-all      Clean everything"
	@echo ""


# Directory targets
releases:
	mkdir releases

dist:
	mkdir dist

DMGS := dist/$(APP_NAME)-$(VERSION).dmg
DMG_RELEASE_TARGS := $(patsubst dist/%,releases/%,$(DMGS))

ZIP := dist/$(APP_NAME)-$(PLATFORM)-$(VERSION).zip
ZIP_RELEASE_TARGS := $(patsubst dist/%,releases/%,$(ZIP))

BINS :=  dist/keeper-$(PLATFORM)-$(VERSION)$(BIN_EXT) dist/keeper-cli-$(PLATFORM)-$(VERSION)$(BIN_EXT)
#RELEASE_TARGS := $(patsubst dist/%,releases/%,$(BINS))

DIRS := build/dmg build dist releases
CLEAN_TARGS := $(patsubst %,clean-%,$(DIRS))

ifeq ($(PLATFORM), Darwin)
	PACKAGE := dmg zip
	RELEASE_TARGS := $(DMG_RELEASE_TARGS) $(ZIP_RELEASE_TARGS)
else ifeq ($(PLATFORM), Windows_NT)
	PACKAGE := zip
	RELEASE_TARGS += $(ZIP_RELEASE_TARGS)
endif

dist/$(APP_NAME).app: $(SRC_FILES) package.spec | dist
	@if [ "$(PLATFORM)" != "Darwin" ]; then echo "macOS only"; exit 1; fi
	uv run pyinstaller --noconfirm package.spec -- --platform Darwin --version $(VERSION)

$(BINS) &: $(SRC_FILES) binaries.spec | dist
	uv run pyinstaller --noconfirm binaries.spec -- --platform $(PLATFORM) --version $(VERSION)

$(DMGS): dist/$(APP_NAME).app | dist
	mkdir -p build/dmg
	cp -R dist/$(APP_NAME).app build/dmg/
	ln -s /Applications build/dmg/Applications

	hdiutil create \
		-volname "$(APP_NAME)" \
		-srcfolder build/dmg \
		-ov \
		-format UDZO \
		$@

$(ZIP): $(BINS) | dist
	uv run python tools/zip_bins.py --output=$@ $^

# High-level targets
.PHONY: bins mac dmg zip release clean-dmg clean-build clean-dist clean-release clean clean-all

mac: dist/$(APP_NAME).app
dmg: $(DMGS)

bins: $(BINS)
zip: $(ZIP)
package: $(PACKAGE)

releases/%: dist/% | releases
	cp $< $@

release: $(RELEASE_TARGS)

$(CLEAN_TARGS):
	rm -rf $(patsubst clean-%,%,$@)

clean-dmg: clean-build/dmg

clean: clean-dmg clean-build clean-dist

clean-all: $(CLEAN_TARGS)
