# Сборка диссертации и статей. Локально доступен только tectonic (XeTeX).
# Первый прогон скачивает пакеты из бандла TeX Live — нужен интернет.

TECTONIC ?= tectonic
BUILD    ?= build
PAPERS   := $(wildcard tex/papers/*/main.tex)

.PHONY: all diss papers clean $(PAPERS)

all: diss papers

diss:
	mkdir -p $(BUILD)/diss
	$(TECTONIC) -X compile tex/diss/main.tex --outdir $(BUILD)/diss --keep-intermediates --keep-logs --synctex

papers: $(PAPERS)

$(PAPERS):
	mkdir -p $(BUILD)/$(notdir $(patsubst %/,%,$(dir $@)))
	$(TECTONIC) -X compile $@ --outdir $(BUILD)/$(notdir $(patsubst %/,%,$(dir $@))) --keep-intermediates --keep-logs --synctex

clean:
	rm -rf $(BUILD)
