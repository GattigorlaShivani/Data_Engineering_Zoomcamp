Note: Execute everything from folder `Week2`.

# Question 1. Load January 2020 data

```
python homework/parametrized_etl_web_to_gcs.py
```
ANSWER:
447,770

# Question 2. Scheduling with Cron

```
prefect deployment build ./homework/parametrized_etl_web_to_gcs.py:etl_web_to_gcs_parent_flow -n hw_web_to_gcs --cron "0 5 1 * *" --timezone "UTC" -q 'default'
prefect deployment apply etl_web_to_gcs_parent_flow-deployment.yaml
```
ANSWER:
0 5 1 * *

# Question 3. Loading data to BigQuery

```
prefect deployment run etl-web-to-gcs-parent-flow/hw_web_to_gcs -p "months=[2,3]" -p "year=2019" -p "color=yellow"



prefect deployment build ./homework/parametrized_etl_gcs_to_bq.py:etl_gcs_to_bq_parent_flow -n hw_gcs_to_bq -q 'default'
prefect deployment apply etl_gcs_to_bq_parent_flow-deployment.yaml
prefect deployment run etl-gcs-to-bq-parent-flow/hw_gcs_to_bq  -p "months=[2,3]" -p "year=2019" -p "color=yellow"
```
ANSWER: 
14,851,920

# Question 4. Github Storage Block
```
poetry add prefect-github

cd ..
prefect deployment build Week2/homework/parametrized_etl_web_to_gcs.py:etl_web_to_gcs_parent_flow -n hw_web_to_gcs_gh -sb github/w2-h2-gh-block-v01 --path Week2/homework/ -o etl_web_to_gcs_parent_flow-deployment.yaml --apply 
prefect deployment apply etl_web_to_gcs_parent_flow-deployment.yaml
prefect deployment run etl-web-to-gcs-parent-flow/hw_web_to_gcs_gh -p "months=[11]" -p "year=2020" -p "color=green"
rm etl_web_to_gcs_parent_flow-deployment.yaml .prefectignore
rm -r data
cd Week2
```
ANSWER:
88,605

# Question 5. Email or Slack notifications

```
prefect cloud login 

cd ..
prefect deployment build Week2/homework/parametrized_etl_web_to_gcs.py:etl_web_to_gcs_parent_flow -n hw_web_to_gcs_gh -sb github/w2-h2-gh-block-v01 --path Week2/homework/ -o etl_web_to_gcs_parent_flow-deployment.yaml --apply 
prefect deployment apply etl_web_to_gcs_parent_flow-deployment.yaml
rm etl_web_to_gcs_parent_flow-deployment.yaml .prefectignore
prefect deployment run etl-web-to-gcs-parent-flow/hw_web_to_gcs_gh -p "months=[4]" -p "year=2019" -p "color=green"
rm -r data
cd Week2
```
ANSWER: 
514,392

#Question 6 Secrets
ANSWER:
8
