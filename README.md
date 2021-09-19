# pychronus

```python
# Build reqs.txt
poetry export -o reqs.txt

# Build docker image
docker build -t airflow . --build-arg PYTHON_DEPS=reqs.txt --build-arg DAGS_PATH=~/code/pychronus/dags 
```