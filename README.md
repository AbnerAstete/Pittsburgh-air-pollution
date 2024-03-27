## How to run

Run the Docker daemon, then...

## Automatic startup
### MacOS / Linux 
```sh
./start.sh
```

If you get a permission error, check the current permissions of the start.sh script
```sh
ls -l start.sh
```

If the script doesn't have execute permissions, you can add them using the chmod command
```sh
chmod +x start.sh
```

You should then be able to run the initial command.

## Manual Startup (all)


Now edit or create an **.env** file like so
```
AIRFLOW_UID=


_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_GID=0
_PIP_ADDITIONAL_REQUIREMENTS=xlsx2csv==0.7.8 faker==8.12.1 praw==7.7.1
```
Make sure to fill in the missing AIRFLOW_UID value with your local user id `id -u`.


Run the following command to creat the volumes needed in order to send data to airflow:
```sh
mkdir -p ./dags ./logs ./plugins
```

And this **once**:
```sh
docker-compose up airflow-init
```
If the exit code is 0 then it's all good.

### Running

```sh
docker-compose up
```

After it is up, add a new connection:

* Name - postgres_default
* Conn type - postgres
* Host - localhost
* Port - 5432
* Database - airflow
* Username - airflow
* Password - airflow