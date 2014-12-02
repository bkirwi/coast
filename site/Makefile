
PAGES = $(patsubst ../doc/%.md, target/%.html, $(wildcard ../doc/*.md))

all: pages
	
pages: target target/main.css $(PAGES)

target:
	mkdir target

target/%.css: %.css
	cp $^ $@ 

clean:
	rm -r target

target/%.html: ../doc/%.md header.html
	pandoc \
		--css=main.css \
		'--css=http://fonts.googleapis.com/css?family=Roboto:400,300' \
  	'--css=http://fonts.googleapis.com/css?family=Source+Code+Pro:300' \
		--metadata=title-prefix:coast \
		--include-before-body=header.html \
		--standalone $< -o $@
