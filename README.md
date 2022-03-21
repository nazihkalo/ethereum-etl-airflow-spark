# Ethereum ETL Airflow

The repository forks from [ethereum-etl-airflow](https://github.com/blockchain-etl/ethereum-etl-airflow).

## deploy-airflow.sh

- namespace **[required]**: namespace scope for this request
- pg-url **[required]**: url of the postgresql, the format should be `user:password@host:port/db`
- eks-host **[required]**: host url of the AWS EKS (the script just server for the EKS users)
- image-name **[required]**: name of the docker image, like: airflow
- image-tag **[required]**: version tag of the docker image, like: 1.10.15-b11
- build-image: if set, will build a new image by the Dockerfile and use it
