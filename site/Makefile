
PAGES = $(patsubst ../doc/%.md, target/%.html, $(wildcard ../doc/*.md))

IMAGES = target/img/CopyStream.svg

PANDOC = pandoc \
		--css=main.css \
		'--css=http://fonts.googleapis.com/css?family=Roboto:400,300' \
		'--css=http://fonts.googleapis.com/css?family=Source+Code+Pro:300' \
		--metadata=title-prefix:coast \
		--include-before-body=header.html \
		--standalone

all: target target/main.css target/index.html $(IMAGES) $(PAGES)

target:
	mkdir target

target/%.css: %.css
	cp $^ $@ 

clean:
	rm -r target

target/img/%.svg: viz/%.dot
	mkdir -p target/img
	dot -Tsvg -Grankdir=LR -Gsize=6,4 $< > $@

viz/%.dot: ../core/src/main/scala/com/monovore/example/coast/%.scala target
	mkdir -p viz
	cd .. && sbt 'coast-core/runMain com.monovore.example.coast.$* dot site/viz/$*.dot'

.SECONDARY: $(wildcard viz/*.dot)

target/index.html: index.md header.html
	mkdir -p target
	$(PANDOC) $< -o $@

target/%.html: ../doc/%.md header.html
	$(PANDOC) $< -o $@
