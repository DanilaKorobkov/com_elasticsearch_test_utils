import asyncio

from elasticsearch import AsyncElasticsearch


async def test__elasticsearch(
    com_elasticsearch_connection: AsyncElasticsearch,
) -> None:
    index = "my-index"
    await com_elasticsearch_connection.indices.create(index)

    doc = {
        "description": "Long long text",
    }
    await com_elasticsearch_connection.index(
        index,
        body=doc,
        id=1,
    )
    await asyncio.sleep(2)

    search_query: dict = {
        "query": {
            "match_all": {},
        },
    }
    results = await com_elasticsearch_connection.search(
        body=search_query,
        index=index,
    )
    assert results["hits"]["hits"]
