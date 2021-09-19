.PHONY: build
build:
    poetry export -o reqs.txt && \
    docker build -t airflow . \
    --build-arg PYTHON_DEPS=reqs.txt \
    --build-arg DAGS_PATH=~/code/pychronus/dags && \
    rm reqs.txt