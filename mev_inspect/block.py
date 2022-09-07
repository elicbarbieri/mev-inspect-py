import asyncio
import logging
from typing import List, Optional, Tuple

from sqlalchemy import orm
from web3 import Web3

from mev_inspect.schemas.blocks import Block
from mev_inspect.schemas.receipts import Receipt
from mev_inspect.schemas.traces import Trace, TraceType
from mev_inspect.utils import hex_to_int

logger = logging.getLogger(__name__)


async def get_latest_block_number(base_provider) -> int:
    latest_block = await base_provider.make_request(
        "eth_getBlockByNumber",
        ["latest", False],
    )

    return hex_to_int(latest_block["result"]["number"])


async def create_from_block_number(
    w3: Web3,
    block_number: int,
    trace_db_session: Optional[orm.Session],
) -> Block:
    (block_timestamp, base_fee), receipts, traces = await asyncio.gather(
        _find_or_fetch_block(w3, block_number, trace_db_session),
        _find_or_fetch_block_receipts(w3, block_number, trace_db_session),
        _find_or_fetch_block_traces(w3, block_number, trace_db_session),
    )

    miner_address = _get_miner_address_from_traces(traces)

    return Block(
        block_number=block_number,
        block_timestamp=block_timestamp,
        miner=miner_address,
        base_fee_per_gas=base_fee,
        traces=traces,
        receipts=receipts,
    )


async def _find_or_fetch_block(
    w3,
    block_number: int,
    trace_db_session: Optional[orm.Session],
) -> Tuple[int, int]:
    if trace_db_session is not None:
        existing_block_timestamp, existing_base_fee = _find_block(
            trace_db_session, block_number
        )
        if existing_block_timestamp != 0:
            return existing_block_timestamp, existing_base_fee

    return await _fetch_block(w3, block_number)


async def _find_or_fetch_block_receipts(
    w3,
    block_number: int,
    trace_db_session: Optional[orm.Session],
) -> List[Receipt]:
    if trace_db_session is not None:
        existing_block_receipts = _find_block_receipts(trace_db_session, block_number)
        if existing_block_receipts is not None:
            return existing_block_receipts

    return await _fetch_block_receipts(w3, block_number)


async def _find_or_fetch_block_traces(
    w3,
    block_number: int,
    trace_db_session: Optional[orm.Session],
) -> List[Trace]:
    if trace_db_session is not None:
        existing_block_traces = _find_block_traces(trace_db_session, block_number)
        if existing_block_traces is not None:
            return existing_block_traces

    return await _fetch_block_traces(w3, block_number)


async def _fetch_block(w3, block_number: int) -> Tuple[int, int]:
    block_json = await w3.eth.get_block(block_number)
    # Need to find proper value for the pre-london hard fork.  (Wont be running very old backfills yet)
    return (
        block_json["timestamp"],
        block_json["baseFeePerGas"] if block_number >= 12_965_000 else 0,
    )


async def _fetch_block_receipts(w3, block_number: int) -> List[Receipt]:
    receipts_json = await w3.eth.get_block_receipts(block_number)
    return [Receipt(**receipt) for receipt in receipts_json]


async def _fetch_block_traces(w3, block_number: int) -> List[Trace]:
    traces_json = await w3.eth.trace_block(block_number)
    return [Trace(**trace_json) for trace_json in traces_json]


# Find fuctions are still going to be fucked up, need to be fixed before running tracedb
def _find_block(
    trace_db_session: orm.Session,
    block_number: int,
) -> Tuple[int, int]:
    result = trace_db_session.execute(
        "SELECT block_timestamp, base_fee FROM blocks WHERE block_number = :block_number",
        params={"block_number": block_number},
    ).one_or_none()

    if result is None:
        return 0, 0
    else:
        (
            block_timestamp,
            base_fee,
        ) = result
        return block_timestamp, base_fee


def _find_block_traces(
    trace_db_session: orm.Session,
    block_number: int,
) -> Optional[List[Trace]]:
    result = trace_db_session.execute(
        "SELECT raw_traces FROM block_traces WHERE block_number = :block_number",
        params={"block_number": block_number},
    ).one_or_none()

    if result is None:
        return None
    else:
        (traces_json,) = result
        return [Trace(**trace_json) for trace_json in traces_json]


def _find_block_receipts(
    trace_db_session: orm.Session,
    block_number: int,
) -> Optional[List[Receipt]]:
    result = trace_db_session.execute(
        "SELECT raw_receipts FROM block_receipts WHERE block_number = :block_number",
        params={"block_number": block_number},
    ).one_or_none()

    if result is None:
        return None
    else:
        (receipts_json,) = result
        return [Receipt(**receipt) for receipt in receipts_json]


def _get_miner_address_from_traces(traces: List[Trace]) -> Optional[str]:
    for trace in traces:
        if trace.type == TraceType.reward:
            return trace.action["author"]

    return None


def get_transaction_hashes(calls: List[Trace]) -> List[str]:
    result = []

    for call in calls:
        if call.type != TraceType.reward:
            if (
                call.transaction_hash is not None
                and call.transaction_hash not in result
            ):
                result.append(call.transaction_hash)

    return result
