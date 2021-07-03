import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

import attr
import docker
from elasticsearch import AsyncElasticsearch
from yarl import URL


@attr.s(auto_attribs=True, slots=True, frozen=True)
class ElasticSearchConfig:
    host: str
    port: int

    def get_url(self) -> URL:
        return URL.build(
            scheme="http",
            host=self.host,
            port=self.port,
        )


@contextmanager
def elasticsearch_docker_container_upped(
    config: ElasticSearchConfig,
) -> Iterator[None]:
    docker_client = docker.from_env()

    container = docker_client.containers.run(
        image="elasticsearch:7.13.2",
        detach=True,
        ports={
            "9200": (config.host, config.port),
        },
        environment={
            "discovery.type": "single-node",
        },
    )
    try:
        yield
    finally:
        container.remove(force=True)
        docker_client.close()


@asynccontextmanager
async def elasticsearch_connection_upped(
    config: ElasticSearchConfig,
) -> AsyncIterator[AsyncElasticsearch]:
    url = str(config.get_url())
    connection = AsyncElasticsearch(url)
    try:
        yield await _wait_elasticsearch_setup(connection)
    finally:
        await connection.close()


async def _wait_elasticsearch_setup(
    connection: AsyncElasticsearch,
) -> AsyncElasticsearch:
    for _ in range(50):
        if await connection.ping():
            return connection
        await asyncio.sleep(0.5)

    raise RuntimeError("Could not connect to the Elasticsearch")
