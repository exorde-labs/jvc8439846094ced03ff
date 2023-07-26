from jvc8439846094ced03ff import query
from exorde_data.models import Item
import logging
import pytest


@pytest.mark.asyncio


async def test_query():
    params = {
        "max_oldness_seconds": DEFAULT_OLDNESS_SECONDS,
        "maximum_items_to_collect": DEFAULT_MAXIMUM_ITEMS,
        "min_post_length": DEFAULT_MIN_POST_LENGTH
    }
    try:
        async for item in query(params):
            assert isinstance(item, Item)
            logging.info("Post Title: " + item.title)
            logging.info("Post Link: " + item.url)
            logging.info("Date of Post: " + item.created_at)
            logging.info("Post Content: " + item.content)
    except ValueError as e:
        logging.exception(f"Error: {str(e)}")


import asyncio
asyncio.run(test_query())

