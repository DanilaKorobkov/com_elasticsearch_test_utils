# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import asyncio
from typing import AsyncIterator, Final, Iterator

import pytest
from aiohttp.test_utils import unused_port
from elasticsearch import AsyncElasticsearch
from yarl import URL

from .utils import (
    ElasticSearchConfig,
    elasticsearch_connection_upped,
    elasticsearch_docker_container_upped,
)

pytest_plugins: Final = ("aiohttp.pytest_plugin",)


@pytest.fixture(scope="session")
def com_elasticsearch_config() -> ElasticSearchConfig:
    return ElasticSearchConfig(
        host="127.0.0.1",
        port=unused_port(),
    )


@pytest.fixture(scope="session")
def com_elasticsearch_url(
    com_elasticsearch_config: ElasticSearchConfig,
) -> Iterator[URL]:
    with elasticsearch_docker_container_upped(com_elasticsearch_config):
        yield com_elasticsearch_config.get_url()


@pytest.fixture
async def com_elasticsearch_connection(
    com_elasticsearch_url: URL,
    com_elasticsearch_config: ElasticSearchConfig,
    loop: asyncio.AbstractEventLoop,
) -> AsyncIterator[AsyncElasticsearch]:
    assert loop.is_running()

    async with elasticsearch_connection_upped(
        com_elasticsearch_config,
    ) as connection:
        yield connection
