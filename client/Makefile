# Detect OS
ifeq ($(OS),Windows_NT)
	PLATFORM := $(OS)
	BIN_EXT := .exe
else
	PLATFORM := $(shell uname -s)
endif

UNAME_ARCH := $(shell uname -m)
ifeq ($(UNAME_ARCH),x86_64)
	DEB_ARCH := amd64
	RPM_ARCH := x86_64
endif

ifeq ($(UNAME_ARCH),aarch64)
	DEB_ARCH := arm64
	RPM_ARCH := aarch64
endif

define BAD_BUILD
	@echo "Error: Can't build $(1) on $(2)"; exit 1
endef

.PHONY: help

help:
	@echo ""
	@echo "Available high-level targets:"
	@echo ""
	@echo "  bins             Build the binaries for the current OS"
	@echo "  zip              Bundle the binaries for the current OS into a zip"
	@echo "  mac              Build the macOS .app bundle"
	@echo "  dmg              Build the macOS DMG package"
	@echo "  deb              Build the .deb package"
	@echo "  packages         Build all packages for the current OS"
	@echo "  release          Build everything and copy to releases/"
	@echo "  clean            Clean build artifacts"
	@echo "  clean-release    Clean the releases directory"
	@echo "  clean-all        Clean everything"
	@echo ""

APP_NAME := Keeper
VERSION := $(shell uv version --short)
CLI_ENTRY := src/acearchive_keeper/__init__.py
GUI_ENTRY := src/acearchive_keeper/gui.py
SRC_FILES := $(shell git ls-files src)

# Directory targets
releases:
	mkdir releases

dist:
	mkdir dist

build:
	mkdir build

DMGROOT := build/dmg
DMGS := dist/$(APP_NAME)-$(VERSION).dmg
DMG_RELEASE_TARGS := $(patsubst dist/%,releases/%,$(DMGS))

ZIP := dist/$(APP_NAME)-$(PLATFORM)-$(VERSION).zip
ZIP_RELEASE_TARGS := $(patsubst dist/%,releases/%,$(ZIP))

PKGROOT := dist/pkgroot
DEB_CONTROL := dist/control
DEB_CONTROL_TEMPLATE := packaging/deb-control.in
DEB := dist/keeper_$(VERSION)_$(DEB_ARCH).deb
DEB_RELEASE_TARGS := $(patsubst dist/%,releases/%,$(DEB))

CLI_BIN := dist/keeper-cli-$(PLATFORM)-$(VERSION)$(BIN_EXT)
GUI_BIN := dist/keeper-$(PLATFORM)-$(VERSION)$(BIN_EXT)
BINS := $(CLI_BIN) $(GUI_BIN)
#RELEASE_TARGS := $(patsubst dist/%,releases/%,$(BINS))

PNG_SIZES := 16x16 32x32 64x64 128x128 256x256 512x512 1024x1024
PNG_TARGS := $(patsubst %,build/keeper_%.png,$(PNG_SIZES))
ICONSET := build/keeper.iconset
LINUX_ICON_TARGS:= $(patsubst %,$(PKGROOT)/usr/share/icons/hicolor/%/apps/Keeper.png,$(PNG_SIZES))

DIRS := $(DMGROOT) $(PKGROOT) $(PKGROOT)/DEBIAN build dist releases
CLEAN_TARGS := $(patsubst %,clean-%,$(DIRS))

ifeq ($(PLATFORM), Darwin)
	PACKAGES := dmg zip
	RELEASE_TARGS := $(DMG_RELEASE_TARGS) $(ZIP_RELEASE_TARGS)
else ifeq ($(PLATFORM), Linux)
	PACKAGES := zip deb
	RELEASE_TARGS := $(ZIP_RELEASE_TARGS) $(DEB_RELEASE_TARGS)
else ifeq ($(PLATFORM), Windows_NT)
	PACKAGES := zip
	RELEASE_TARGS += $(ZIP_RELEASE_TARGS)
endif

$(PNG_TARGS): assets/keeper.png | build
	magick $< -resize $(patsubst build/keeper_%.png,%,$@) $@

build/keeper.ico: $(PNG_TARGS) | build
	magick convert $^ build/keeper.ico

build/keeper.icns: assets/keeper.png | build
	mkdir -p $(ICONSET)
	magick $^ -resize 16x16     $(ICONSET)/icon_16x16.png
	magick $^ -resize 32x32     $(ICONSET)/icon_16x16@2x.png
	magick $^ -resize 32x32     $(ICONSET)/icon_32x32.png
	magick $^ -resize 64x64     $(ICONSET)/icon_32x32@2x.png
	magick $^ -resize 128x128   $(ICONSET)/icon_128x128.png
	magick $^ -resize 256x256   $(ICONSET)/icon_128x128@2x.png
	magick $^ -resize 256x256   $(ICONSET)/icon_256x256.png
	magick $^ -resize 512x512   $(ICONSET)/icon_256x256@2x.png
	magick $^ -resize 512x512   $(ICONSET)/icon_512x512.png
	magick $^ -resize 1024x1024 $(ICONSET)/icon_512x512@2x.png
	iconutil --convert icns --output $@ $(ICONSET)

dist/$(APP_NAME).app: $(SRC_FILES) package.spec build/keeper.icns | dist
ifeq ($(PLATFORM), Darwin)
	uv run pyinstaller --noconfirm package.spec -- --platform Darwin --version $(VERSION)
else
	$(call BAD_BUILD,$@,$(PLATFORM))
endif

$(BINS) &: $(SRC_FILES) binaries.spec build/keeper.ico | dist
	uv run pyinstaller --noconfirm binaries.spec -- --platform $(PLATFORM) --version $(VERSION)

$(DMGS): dist/$(APP_NAME).app | dist
	mkdir -p $(DMGROOT)
	cp -R dist/$(APP_NAME).app $(DMGROOT)/
	ln -s /Applications $(DMGROOT)/Applications

	hdiutil create \
		-volname "$(APP_NAME)" \
		-srcfolder $(DMGROOT) \
		-ov \
		-format UDZO \
		$@

$(ZIP): $(BINS) | dist
	uv run python tools/zip_bins.py --output=$@ $^

$(LINUX_ICON_TARGS): $(PNG_TARGS)
	@mkdir -p $(dir $@)
	cp $(patsubst $(PKGROOT)/usr/share/icons/hicolor/%/apps/Keeper.png,build/keeper_%.png,$@) $@

$(DEB_CONTROL): $(DEB_CONTROL_TEMPLATE) | build
	@mkdir -p $(dir $@)
	sed -e "s/@VERSION@/$(VERSION)/" \
		-e "s/@ARCH@/$(DEB_ARCH)/" \
		$< > $@

$(DEB): $(BINS) $(DEB_CONTROL) $(LINUX_ICON_TARGS)
ifeq ($(PLATFORM), Linux)
	mkdir -p $(PKGROOT)/DEBIAN
	mkdir -p $(PKGROOT)/usr/bin
	mkdir -p $(PKGROOT)/usr/lib/keeper
	mkdir -p $(PKGROOT)/usr/share/applications
	cp $(DEB_CONTROL) $(PKGROOT)/DEBIAN/control
	cp $(CLI_BIN) $(PKGROOT)/usr/bin/keeper
	cp $(GUI_BIN) $(PKGROOT)/usr/lib/keeper/Keeper
	cp assets/keeper.desktop $(PKGROOT)/usr/share/applications/Keeper.desktop
	dpkg-deb --build $(PKGROOT) $(DEB)
else
	$(call BAD_BUILD,$@,$(PLATFORM))
endif

# High-level targets
.PHONY: bins mac dmg zip deb packages mac-icons linux-icons win-icons release clean-dmg clean-build clean-dist clean-release clean clean-all

bins: $(BINS)
zip: $(ZIP)
mac: dist/$(APP_NAME).app
dmg: $(DMGS)
deb: $(DEB)
packages: $(PACKAGES)
mac-icons: build/keeper.icns
linux-icons: $(LINUX_ICON_TARGS)
win-icons: build/keeper.ico
icons: win-icons mac-icons linux-icons

releases/%: dist/% | releases
	cp $< $@

release: $(RELEASE_TARGS)

$(CLEAN_TARGS):
	rm -rf $(patsubst clean-%,%,$@)

clean-dmg: clean-$(DMGROOT)
clean-pkgroot: clean-build/pkgroot
clean-release: clean-releases

clean: clean-dmg clean-build clean-dist

clean-all: $(CLEAN_TARGS)
