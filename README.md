# pychronus

```python
# Build reqs.txt
poetry export -o reqs.txt

# Build docker image
docker build -t airflow . --build-arg PYTHON_DEPS=reqs.txt --build-arg DAGS_PATH=~/code/pychronus/dags
```


## Create User

You will be able to create a user account by accessing shell in the running webserver container.

```shell
docker exec -it airflow_webserver_1 sh
```

Once in the shell of the running docker container:

```shell
airflow users create \
  --username jaime \
  --firstname jaime \
  --lastname mendoza \
  --passsword admin \
  --role Admin \
  --email jaimegm49@gmail.com
```

```code

# Templated postgql connection string
sql_alchemy_conn=postgresql+psycopg2://{$username}:{$password}@{$host}/{$database_name}

```

airflow users create \
  -u jaime \
  -f jaime \
  -l mendoza \
  -p admin \
  -r Admin \
  -e jaimegm49@gmail.com
