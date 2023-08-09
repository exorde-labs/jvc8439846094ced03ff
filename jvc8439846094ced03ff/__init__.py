"""
In this script we are going to collect data from Seeking Alpha. We will navigate to links of this type:

https://www.jeuxvideo.com/forums/0-3011927-0-1-0-1-0-finance.htm

Once on it, we can extract all the latest news posts.

A simple GET request will return the page. We can then perform a lookup for all the elements following this structure:

<span class="topic-date"/> :: collect the date and check for HH:MM:SS format and that the date respects our time frame

If this passes collect:

span.parent.find("a", {"class": "lien-jv topic-title stretched-link"})

--> ["href"] will yield the link to the specific forum
--> .text will yield the title of the forum

With this link, we can navigate to the forum's page, find the last page of the forum and collect the last post.

Once on the forum page look for this element:

<div class="bloc-liste-num-page"/>

And get all the direct children objects for this element.

IF there is only one --> we are on the last page

IF there are more than one cycle through each of them until you come to the last one OR until the text matches "»"
    --> we can do this simply with string.isnumeric()

If no "»" character is found, then the last element is the final page to which we must navigate. Otherwise, the last
element will be the before last element of the list.

The final page can be accessed this way:

https://www.jeuxvideo.com/forums/[id]-[page_number]-0-1-0-[title].htm

A bit of tweaking is required here to create the right URL and navigate to the last page.

Once on this last page look for all the elements of type:

<div class="bloc-message-forum mx-2 mx-lg-0"/> --> start with the last one and go up until we hit our time window limit
or run out of pages to scrape
    --> get data-id to recompose the url to the message as follows: https://www.jeuxvideo.com/forums/message/[data-id]

    <div class="txt-msg  text-enrichi-forum "/>.text for the content
    <span class="... text-user"/>.text for the author of the post
    <div class="bloc-date-msg"/>.text for the date of the post under this format: "Day Month YYYY [à] HH:MM:SS"

"""
import re
import aiohttp
import random
import logging
from bs4 import BeautifulSoup
from typing import AsyncGenerator
from datetime import datetime, timedelta
import pytz
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId
)

# GLOBAL VARIABLES
USER_AGENT_LIST = [
    'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]
DEFAULT_OLDNESS_SECONDS = 240
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10

RANDOM_SKIP_TOPIC_PROBABILITY = 0.20

TIMESTAMP_PATTERN = r'^\d{2}:\d{2}:\d{2}$'

JVc_URLS = [
    "https://www.jeuxvideo.com/forums/0-51-0-1-0-1-0-blabla-18-25-ans.htm",
    "https://www.jeuxvideo.com/forums/0-52-0-1-0-1-0-blabla-25-35-ans.htm",
    "https://www.jeuxvideo.com/forums/0-53-0-1-0-1-0-blabla-35-ans-et-plus.htm",
    "https://www.jeuxvideo.com/forums/0-1000034-0-1-0-1-0-japon.htm",
    "https://www.jeuxvideo.com/forums/0-83-0-1-0-1-0-quebec.htm",
    "https://www.jeuxvideo.com/forums/0-1000022-0-1-0-1-0-suisse.htm",
    "https://www.jeuxvideo.com/forums/0-1000020-0-1-0-1-0-belgique.htm",
    "https://www.jeuxvideo.com/forums/0-3011927-0-1-0-1-0-finance.htm",
    "https://www.jeuxvideo.com/forums/0-69-0-1-0-1-0-actualites.htm",
    "https://www.jeuxvideo.com/forums/0-3002340-0-1-0-1-0-sante-et-bien-etre.htm",
    "https://www.jeuxvideo.com/forums/0-3000405-0-1-0-1-0-creation.htm",
    "https://www.jeuxvideo.com/forums/0-3000473-0-1-0-1-0-loisirs.htm",
    "https://www.jeuxvideo.com/forums/0-65-0-1-0-1-0-sciences-technologies.htm",
    "https://www.jeuxvideo.com/forums/0-3000481-0-1-0-1-0-savoir-culture.htm",
    "https://www.jeuxvideo.com/forums/0-24-0-1-0-1-0-sport.htm",
    "https://www.jeuxvideo.com/forums/0-1-0-1-0-1-0-informatique.htm",
    "https://www.jeuxvideo.com/forums/0-3000397-0-1-0-1-0-jeux-pc.htm",
    "https://www.jeuxvideo.com/forums/0-7-0-1-0-1-0-general-jeux-video.htm"
]

FRENCH_MONTHS_TO_NUMBERS = {
    'janvier': '01',
    'février': '02',
    'mars': '03',
    'avril': '04',
    'mai': '05',
    'juin': '06',
    'juillet': '07',
    'août': '08',
    'septembre': '09',
    'octobre': '10',
    'novembre': '11',
    'décembre': '12'
}


async def fetch_page(session, url):
    async with session.get(url, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=8.0) as response:
        return await response.text()


async def request_last_page(_url, _max_age, _post_title, _bypass=False):
    try:
        async with aiohttp.ClientSession() as session:
            response = await fetch_page(session, _url)
            soup = BeautifulSoup(response, 'html.parser')

            # we are finally on the last page of the forum that is relevant to our search, we can begin collecting the comments
            """
            Once on this last page look for all the elements of type:
    
            <div class="bloc-message-forum mx-2 mx-lg-0"/> --> start with the last one and go up until we hit our time window limit
            or run out of pages to scrape
                --> get data-id to recompose the url to the message as follows: https://www.jeuxvideo.com/forums/message/[data-id]
            
                <div class="txt-msg  text-enrichi-forum "/>.text for the content
                <span class="... text-user"/>.text for the author of the post
                <div class="bloc-date-msg"/>.text for the date of the post under this format: "Day Month YYYY [à] HH:MM:SS"
            """
            cards = soup.find_all("div", {"class": "bloc-message-forum mx-2 mx-lg-0"})  # search it backwards, start by the latest
            for card in reversed(cards):
                date_of_post = card.findChild("div", {"class": "bloc-date-msg"}).text.strip()  # Date needs to be converted
                post_date = convert_date_and_time_to_date_format(date_of_post)
                if not check_for_max_age_with_correct_format(post_date, _max_age):
                    yield None

                data_id = card["data-id"]
                # url = "https://www.jeuxvideo.com/forums/message/" + data_id
                url = _url + "?exorde-internal-id:" + data_id  # optional parameter to find the comment later

                # Some posts will be responding to others
                post_content = card.findChild("div", {"class": "txt-msg text-enrichi-forum"})
                contents = post_content.findChildren("p", recursive=False)
                content = _post_title + ".\n"
                for i in range(0, len(contents)):
                    content += contents[i].text
                    if i != len(contents)-1:
                        content += "\n"  # skip line to respect paragraph indenting
                yield Item(
                    title=Title(_post_title),
                    content=Content(content),
                    created_at=CreatedAt(post_date),
                    url=Url(url),
                    domain=Domain("jeuxvideo.com"),
                    external_id=ExternalId(str(data_id))
                )
                # verify if there are any comments to this post
                # if there are, we need to get them too


    except Exception as e:
        logging.exception("Error:" + str(e))


async def request_content_with_timeout(_url, _max_age, _post_title):
    """
    Returns all relevant information from the news post
    :param _post_title: the title of the post to which the comment is linked
    :param _max_age: the maximum age we will allow for the post in seconds
    :param _url: the url of the post
    :return: the content of the post

    Once on the forum page look for this element:

    <div class="bloc-liste-num-page"/>
    """
    try:
        async with aiohttp.ClientSession() as session:
            response = await fetch_page(session, _url)
            soup = BeautifulSoup(response, 'html.parser')

            find_last_page = soup.find("div", {"class": "bloc-liste-num-page"})
            children = find_last_page.findChildren(recursive=False)

            if len(children) == 1:  # we are on the last page, carry on
                async for item in request_last_page(_url, _max_age, _post_title):
                    yield item
            else:  # find the last element of the forum
                url_tab = _url.split("-1-0-1-0-")
                if children[len(children)-1].text.isnumeric():  # this means that the last element is a number, therefore the page we are looking for
                    url = url_tab[0] + "-" + children[len(children)-1].text + "-0-1-0-" + url_tab[1]
                    async for item in request_last_page(url, _max_age, _post_title):
                        yield item
                else:  # there are more than 11 pages, so the last element in that block will be "»", get the one before
                    url = url_tab[0] + "-" + children[len(children)-2].text + "-0-1-0-" + url_tab[1]
                    async for item in request_last_page(url, _max_age, _post_title):
                        yield item
    except Exception as e:
        logging.exception("Error:" + str(e))


async def request_entries_with_timeout(_url, _max_age):
    """
    Extracts all card elements from the latest news section
    :param _max_age: the maximum age we will allow for the post in seconds
    :param _url: the url where we will find the latest posts
    :return: the card elements from which we can extract the relevant information

    Look for <span class="topic-date"/> :: collect the date and check for HH:MM:SS format and that the date respects our time frame
    """
    try:
        async with aiohttp.ClientSession() as session:
            response = await fetch_page(session, _url)
            soup = BeautifulSoup(response, 'html.parser')
            unfiltered_entries = soup.find_all("span", {"class": "topic-date"})
            entries = []
            for entry in unfiltered_entries:
                if re.match(TIMESTAMP_PATTERN, entry.text.strip()):  # now check that it respects our time window
                    if check_for_max_age(entry.text.strip(), _max_age):
                        if random.uniform(0, 1) >= RANDOM_SKIP_TOPIC_PROBABILITY:  # random chance to skip the topic
                            entries.append(entry)
            async for item in parse_entry_for_elements(entries, _max_age):
                yield item
    except Exception as e:
        logging.exception("Error:" + str(e))


def convert_date_and_time_to_date_format(_date):
    """
    "Day Month YYYY [à] HH:MM:SS" to standard
    :param _date: Day Month YYYY [à] HH:MM:SS
    :return: correctly formated date
    """
    # Remove the word "à" and split the date and time parts
    date_part, time_part = _date.split("à")
    # Extract day, month, and year from the date part
    day, month_french, year = date_part.split()
    # Convert French month name to month number
    month_number = FRENCH_MONTHS_TO_NUMBERS[month_french]
    # Combine the components to form a date string in the format "YYYY-MM-DD"
    date_string = f"{year}-{month_number}-{day}"
    # Combine the date and time strings
    datetime_str = f"{date_string} {time_part}"
    # Parse the combined string into a datetime object
    input_time = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S") - timedelta(hours=2)  # convert to UTC + 0
    # Convert to UTC+0 (UTC) and format to the desired string format
    formatted_time = input_time.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    return formatted_time


def convert_to_date_format(_date):
    # Get today's date
    today_date = datetime.now().date()
    # Combine today's date and the provided timestamp
    combined_str = f"{today_date}T{_date}"
    date_to_check = datetime.strptime(combined_str, "%Y-%m-%dT%H:%M:%S") - timedelta(
        hours=2)  # convert from UTC + 1 to UTC + 0
    return date_to_check.strftime(date_to_check, "%Y-%m-%dT%H:%M:%S.00Z")


def check_for_max_age_with_correct_format(_date, _max_age):
    date_to_check = datetime.strptime(_date, "%Y-%m-%dT%H:%M:%S.00Z")
    now_time = datetime.strptime(datetime.strftime(datetime.now(pytz.utc), "%Y-%m-%dT%H:%M:%S.00Z"),
                                 "%Y-%m-%dT%H:%M:%S.00Z")
    if (now_time - date_to_check).total_seconds() <= _max_age:
        return True
    else:
        return False


def check_for_max_age(_date, _max_age):
    """
    Checks if the entry is within the max age bracket that we are looking for
    :param _date: the datetime from the entry
    :param _max_age: the max age to which we will be comparing the timestamp
    :return: true if it is within the age bracket, false otherwise
    """
    # Get today's date
    today_date = datetime.now().date()
    # Combine today's date and the provided timestamp
    combined_str = f"{today_date}T{_date}"
    date_to_check = datetime.strptime(combined_str, "%Y-%m-%dT%H:%M:%S") - timedelta(
        hours=2)  # convert from UTC + 1 to UTC + 0
    now_time = datetime.strptime(datetime.strftime(datetime.now(pytz.utc), "%Y-%m-%dT%H:%M:%S.00Z"),
                                 "%Y-%m-%dT%H:%M:%S.00Z")

    if (now_time - date_to_check).total_seconds() <= _max_age:
        return True
    else:
        return False


async def parse_entry_for_elements(_cards, _max_age):
    """
    Parses every card element to find the relevant links & titles to the connected forums
    :param _max_age: The maximum age we will allow for the post in seconds
    :param _cards: The parent card objects from which we will be gathering the information
    :return: All the parameters we need to return an Item instance

    GET span.parent.find("a", {"class": "lien-jv topic-title stretched-link"})
    """
    try:
        for card in _cards:
            parent = card.parent.find("a", {"class": "lien-jv topic-title stretched-link"})
            post_title = parent.text.strip()
            async for item in request_content_with_timeout("https://www.jeuxvideo.com" + parent["href"], _max_age, post_title):
                if item:
                    yield item
                else:
                    break  # if this item was not in the time bracket that interests us, the following ones will not be either
    except Exception as e:
        logging.exception("Error:" + str(e))


def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return max_oldness_seconds, maximum_items_to_collect, min_post_length


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    url_main_endpoint = random.choice(JVc_URLS)  # choose a URL in the given list at random
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[JeuxVideo.com] - Scraping messages posted less than {max_oldness_seconds} seconds ago.")

    async for item in request_entries_with_timeout(url_main_endpoint, max_oldness_seconds):
        yielded_items += 1
        yield item
        logging.info(f"[JeuxVideo.com] Found new post :\t {item.title}, posted at {item.created_at}, URL = {item.url}")
        if yielded_items >= maximum_items_to_collect:
            break
