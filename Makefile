NAME=ContextServer

all: test

dev-env:
	rm -rf venv
	virtualenv -p python3 venv

build:
	pip3 install -r requirements.txt

test: build
	python3 app.py
