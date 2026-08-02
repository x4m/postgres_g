# Сборка диссертации и статей. Локально доступен только tectonic (XeTeX).
# Первый прогон скачивает пакеты из бандла TeX Live — нужен интернет.

TECTONIC ?= tectonic
BUILD    ?= build
PAPERS   := $(wildcard tex/papers/*/main*.tex)

.PHONY: all diss papers titlepages clean $(PAPERS)

all: diss papers titlepages

# Титульные страницы: системы подачи Elsevier принимают только doc и docx,
# поэтому источник — HTML, а .docx получается конвертацией.
titlepages:
	@for f in tex/papers/*/title-page.html; do \
	  [ -e "$$f" ] || continue; \
	  textutil -convert docx -output "$${f%.html}.docx" "$$f" && echo "собрано $${f%.html}.docx"; \
	done

diss:
	mkdir -p $(BUILD)/diss
	$(TECTONIC) -X compile tex/diss/main.tex --outdir $(BUILD)/diss --keep-intermediates --keep-logs --synctex

papers: $(PAPERS)

$(PAPERS):
	mkdir -p $(BUILD)/$(notdir $(patsubst %/,%,$(dir $@)))
	$(TECTONIC) -X compile $@ --outdir $(BUILD)/$(notdir $(patsubst %/,%,$(dir $@))) --keep-intermediates --keep-logs --synctex

clean:
	rm -rf $(BUILD)
