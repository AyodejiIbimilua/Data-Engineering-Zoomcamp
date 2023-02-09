from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from assignment_flow import etl_assignment_flow

docker_dep = Deployment.build_from_flow(
    flow=etl_assignment_flow,
    name="assignment-flow"
)

if __name__ == "__main__":
    docker_dep.apply()