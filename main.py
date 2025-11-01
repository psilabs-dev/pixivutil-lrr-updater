import argparse
import asyncio
import datetime
import enum
import json
import logging
import os
from pathlib import Path
import re
from typing import Dict, List
import aiohttp.client_exceptions
import aiosqlite
from lanraragi import LRRClient
from lanraragi.models.archive import GetArchiveMetadataRequest, UpdateArchiveMetadataRequest

# TODO: file logging.
logger = logging.getLogger(__name__)

class TaskResult(enum.Enum):

    SUCCESS = "SUCCESS"
    PIXIVUTIL_NO_METADATA = "PIXIVUTIL_NO_METADATA"

# sanitize the text according to the search syntax: https://sugoi.gitbook.io/lanraragi/basic-operations/searching
def sanitize_tag(text: str) -> str:
    sanitized = text
    
    # replace nonseparator characters with empty str. (", ?, *, %, $, :)
    sanitized = re.sub(r'["?*%$:]', '', sanitized)

    # replace underscore with space.
    sanitized = sanitized.replace('_', ' ')

    # if a dash is preceded by space, remove; otherwise, keep.
    sanitized = sanitized.replace(' -', ' ')

    if sanitized != text:
        logger.info(f"\"{text}\" was sanitized.")

    return sanitized

async def update_archive_metadata(
    arcid: str, lrr: LRRClient, db: aiosqlite.Connection
) -> TaskResult:
    """
    Apply tags (and en tag translations) from PixivUtil2 (server) database
    to the LRR server. Copies summary, title, and tags over, with possibly a `date_added`
    tag from LRR.

    NOTE: this currently ONLY applies to untagged archives.
    To handle tagged archives, we need to handle namespaced tags as well.
    """

    # get metadata
    response, err = await lrr.archive_api.get_archive_metadata(GetArchiveMetadataRequest(arcid=arcid))
    if err:
        raise Exception(f"Failed to get archive metadata: {err.error}")
    tags = response.tags # there may be a date_added tag.
    
    filename = response.filename
    pixiv_id = filename.split("pixiv_")[1].strip()
    if not pixiv_id.isdigit():
        raise Exception(f"Invalid Pixiv ID format from filename: {filename}")
    
    # see if entry exists.
    pixiv_id = int(pixiv_id)
    master_row = await (await db.execute(
        'SELECT image_id, member_id, title, caption FROM pixiv_master_image WHERE image_id = ?', (pixiv_id,))
    ).fetchone()
    if not master_row:
        return TaskResult.PIXIVUTIL_NO_METADATA
    
    # get basic info.
    image_id, member_id, title, caption = master_row
    summary = caption
    tag_list = [tag.strip() for tag in tags.split(",")]

    # add source info
    tag_list.append(f"source:https://pixiv.net/artworks/{image_id}")

    # get artist/member info
    tag_list.append(f"pixiv_user_id:{member_id}")
    member_row = await (await db.execute(
        "SELECT name FROM pixiv_master_member WHERE member_id = ?", (member_id,)
    )).fetchone()
    if not member_row:
        raise Exception(f"Pixiv artwork has no member: arcid={arcid}, pixiv_id={pixiv_id}")
    member_name: str = member_row[0]
    tag_list.append(f"artist:{sanitize_tag(member_name)}")

    # get image to tag info
    i2t_rows = await (await db.execute(
        'SELECT tag_id FROM pixiv_image_to_tag WHERE image_id = ?', (pixiv_id,)
    )).fetchall()
    for i2t in i2t_rows:
        tag_id = i2t[0]
        tag_list.append(sanitize_tag(tag_id))

        # get en translation (if exists)
        entl_row = await (await db.execute(
            "SELECT translation FROM pixiv_tag_translation WHERE translation_type = 'en' AND tag_id = ?", (tag_id,)
        )).fetchone()
        if entl_row:
            en_translation = entl_row[0]
            tag_list.append(sanitize_tag(en_translation))

    # get create and update info
    date_row = await (await db.execute(
        "SELECT created_date_epoch, uploaded_date_epoch FROM pixiv_date_info WHERE image_id = ?", (pixiv_id,)
    )).fetchone()

    if date_row:
        created_date_epoch: int = int(date_row[0])
        uploaded_date_epoch: int = int(date_row[1])
        tag_list.append("date_created:" + str(created_date_epoch))
        tag_list.append("date_uploaded:" + str(uploaded_date_epoch))

    tags = ",".join(set(tag_list))

    # prepare request
    retry_count = 0
    max_retries = 3
    while True:
        try:
            response, err = await lrr.archive_api.update_archive_metadata(UpdateArchiveMetadataRequest(
                arcid=arcid, title=title, tags=tags, summary=summary
            ))
            if err and err.status == 423: # locked resource
                if retry_count > max_retries:
                    raise Exception(f"Persistent lock problem encountered while updating archive {arcid}: {err.error}")
                tts = 2 ** (retry_count+1)
                await asyncio.sleep(tts)
                retry_count += 1
                continue
            elif err:
                raise Exception(f"Failed to update archive metadata: {err.error}")
            break
        except (
            aiohttp.client_exceptions.ClientConnectionError,
            aiohttp.client_exceptions.ClientConnectorDNSError,
            asyncio.TimeoutError,
        ):
            # temporary network issue
            if retry_count > max_retries:
                raise
            tts = 2 ** (retry_count+1)
            await asyncio.sleep(tts)
            retry_count += 1
            continue
    
    return TaskResult.SUCCESS

async def update_lrr_metadata(lrr: LRRClient, db: aiosqlite.Connection) -> Dict[str, List[str]]:
    
    result_mapping: Dict[str, List[str]] = {}

    # validation stage and establish connection to LRR.
    _, err = await lrr.shinobu_api.get_shinobu_status()
    if err:
        raise Exception(f"Failed to confirm API key validity: {err.error}")
    
    # validate db has required tables.
    required_tables = [
        "pixiv_master_image",
        "pixiv_manga_image",
        "pixiv_date_info",
        "pixiv_image_to_tag",
        "pixiv_master_member",
        "pixiv_master_tag",
        "pixiv_tag_translation",
    ]
    for tb_name in required_tables:
        if not await (
            await db.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (tb_name,))
        ).fetchone():
            raise Exception(f"Required table does not exist: {tb_name}")

    # get untagged archives
    logger.info("Getting untagged archives...")
    response, err = await lrr.archive_api.get_untagged_archives()
    if err:
        raise Exception(f"Failed to get untagged archives: {err.error}")
    arcids = response.data
    
    for arcid in arcids:
        result = await update_archive_metadata(arcid, lrr, db)
        if result.value not in result_mapping:
            result_mapping[result.value] = []
        result_mapping[result.value].append(arcid)
        logger.info(f"[{arcid}] update status: {result.value}")
    return result_mapping

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lrr", type=str, required=True, help="LANraragi base URL.")
    parser.add_argument("--db", type=str, required=True, help="Path to pixivutil2 sqlite database.")
    args = parser.parse_args()
    db_path: str = args.db
    lrr_base_url: str = args.lrr

    lrr_api_key: str = os.getenv("LRR_API_KEY")
    logging.basicConfig(level=logging.INFO)

    assert lrr_api_key is not None
    assert Path(db_path).exists(), f"Database does not exist: {db_path}"

    result_mapping: Dict[str, List[str]] = {}
    async with (
        LRRClient(lrr_base_url, lrr_api_key=lrr_api_key) as lrr,
        aiosqlite.connect(db_path) as db
    ):
        result_mapping = await update_lrr_metadata(lrr, db)
    
    datestr = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    output_file = Path("results") / f"update-job-{datestr}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as writer:
        json.dump(result_mapping, writer)
    logger.info(f"Updated archives and wrote results to {output_file}.")

if __name__ == "__main__":
    asyncio.run(main())
