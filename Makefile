.PHONY: build
build:
    poetry export -o reqs.txt && \
    docker build -t airflow . \
    --build-arg PYTHON_DEPS=reqs.txt && \
    rm reqs.txt
