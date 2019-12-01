SHELL = /bin/bash

all : render
.PHONY : all

render:
	( \
		mkdir -p deployment-dags; \
		python render.py templates/ manifests/ deployment-dags/; \
	) \

clean:
	rm -rf deployment-dags
