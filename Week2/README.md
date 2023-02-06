## Start Prefect Orion
```
prefect orion start
```

## Register prefect block for GCP bucket
``` 
prefect block register -m prefect_gcp
```

## Build  Prefect deployment
As a starting point, flow uses function `etl_parent_flow` from `parametrized_flow.py`:
```
prefect deployment build ./flows/gcp/parametrized_flow.py:etl_parent_flow -n "Parametrized ETL"
```
Output is the flow metadata stored at `etl_parent_flow-deployment.yaml`. Using the following command metadata is sent to Prefect API so Prefect now knows how to orcherstrate this flow:
```
prefect deployment apply etl_parent_flow-deployment.yaml 
```

Build deployment with the scheduler:
```
prefect deployment build ./flows/deployments/parametrized_flow.py:etl_parent_flow -n etl2 --cron "0 0 * * *" -q 'default'
```

## Dockerizing Prefect code

```
USERNAME=<your_username>
docker login -u $USERNAME
docker image build -t $USERNAME/de-zoomcamp:prefect-zoom -f .Dockerfile .
docker push $USERNAME/de-zoomcamp:prefect-zoom
```

```
prefect profile ls
# Use Orion server API url instad of ephermal Prefect API
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

```
prefect agent start -q default
prefect deployment run etl-parent-flow/docker-flow -p "months=[1,2]"
```