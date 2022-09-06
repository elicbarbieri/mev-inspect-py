load("ext://helm_remote", "helm_remote")
load("ext://secret", "secret_from_dict")
load("ext://configmap", "configmap_from_dict")
load('ext://dotenv', 'dotenv')
dotenv()

helm_remote("redis",
            repo_name="bitnami",
            repo_url="https://charts.bitnami.com/bitnami",
            set=["global.redis.password=password"],
)

k8s_yaml(secret_from_dict("mev-inspect-db-credentials", inputs = {
    "username" : os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "host": os.environ["POSTGRES_HOST"],
}))

# if using https://github.com/taarushv/trace-db
# k8s_yaml(secret_from_dict("trace-db-credentials", inputs = {
#    "username" : os.environ["TRACE_DB_USER"],
#    "password": os.environ["TRACE_DB_PASSWORD"],
#    "host": os.environ["TRACE_DB_HOST"],
# }))

k8s_yaml(configmap_from_dict("mev-inspect-rpc", inputs = {
    "url" : os.environ["RPC_URL"],
}))

k8s_yaml(configmap_from_dict("mev-inspect-listener-healthcheck", inputs = {
    "url" : os.getenv("LISTENER_HEALTHCHECK_URL", default=""),
}))

docker_build("mev-inspect-py", ".",
    live_update=[
        sync(".", "/app"),
        run("cd /app && poetry install",
            trigger="./pyproject.toml"),
    ],
)

k8s_yaml(helm(
    './k8s/mev-inspect',
    name='mev-inspect',
    set=[
        "extraEnv[0].name=AWS_ACCESS_KEY_ID",
        "extraEnv[0].value=foobar",
        "extraEnv[1].name=AWS_SECRET_ACCESS_KEY",
        "extraEnv[1].value=foobar",
        "extraEnv[2].name=AWS_REGION",
        "extraEnv[2].value=us-east-1",
        "extraEnv[3].name=AWS_ENDPOINT_URL",
        "extraEnv[3].value=http://localstack:4566",
    ],
))

k8s_yaml(helm(
    './k8s/mev-inspect-workers',
    name='mev-inspect-workers',
    set=[
        "extraEnv[0].name=AWS_ACCESS_KEY_ID",
        "extraEnv[0].value=foobar",
        "extraEnv[1].name=AWS_SECRET_ACCESS_KEY",
        "extraEnv[1].value=foobar",
        "extraEnv[2].name=AWS_REGION",
        "extraEnv[2].value=us-east-1",
        "extraEnv[3].name=AWS_ENDPOINT_URL",
        "extraEnv[3].value=http://localstack:4566",
        "replicaCount=1",
    ],
))

k8s_resource(
    workload="mev-inspect",
    resource_deps=["redis-master"],
)

k8s_resource(
    workload="mev-inspect-workers",
    resource_deps=["redis-master"],
)

# uncomment to enable price monitor
# k8s_yaml(helm('./k8s/mev-inspect-prices', name='mev-inspect-prices'))
# k8s_resource(workload="mev-inspect-prices", resource_deps=[])


# if using local S3 exports
#k8s_yaml(secret_from_dict("mev-inspect-export", inputs = {
#    "export-bucket-name" : "local-export",
#    "export-bucket-region": "us-east-1",
#    "export-aws-access-key-id": "foobar",
#    "export-aws-secret-access-key": "foobar",
#}))

#helm_remote(
#    "localstack",
#    repo_name="localstack-charts",
#    repo_url="https://localstack.github.io/helm-charts",
#)
#
#local_resource(
#    'localstack-port-forward',
#    serve_cmd='kubectl port-forward --namespace default svc/localstack 4566:4566',
#    resource_deps=["localstack"]
#)
#
#k8s_yaml(configmap_from_dict("mev-inspect-export", inputs = {
#    "services": "s3",
#}))
