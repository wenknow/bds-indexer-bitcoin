import os
from node.node import BitcoinNode
from utils import save_hash_table
from setup_logger import setup_logger
from node.node_utils import parse_block_data
from dotenv import load_dotenv
import time
import multiprocessing
from functools import partial

load_dotenv()
logger = setup_logger("Indexer")


def get_block_with_retry(bitcoin_node, block_height, retries=30, delay=2):
    for attempt in range(retries):
        res = bitcoin_node.get_block_by_height(block_height)
        if res is not None:
            return res
        logger.error(f"Error getting block {block_height}, attempt {attempt + 1}/{retries}")
        time.sleep(delay)


def deal_one_block(_bitcoin_node, block_height):
    block_table = {}
    try:
        block = get_block_with_retry(_bitcoin_node, block_height)
        block_data = parse_block_data(block)
        transactions = block_data.transactions

        for tx in transactions:
            in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, out_total_amount = _bitcoin_node.process_in_memory_txn_for_indexing(tx)
            block_table[tx.tx_id] = {
                'in_amount_by_address': in_amount_by_address,
                'out_amount_by_address': out_amount_by_address,
                'input_addresses': input_addresses,
                'output_addresses': output_addresses,
                'in_total_amount': in_total_amount,
                'out_total_amount': out_total_amount,
                'tx_info': {
                    "timestamp": tx.timestamp,
                    "block_height": tx.block_height,
                    "is_coinbase": tx.is_coinbase,
                }
            }
        if block_height % 100 == 0:
            logger.info(f"success deal block: {block_height}")
    except Exception as e:
        logger.error(f"Error deal_one_block2 {block_height} : {e}")

    return block_table


def deal(bitcoin_node, start_block, end_block):

    deal_table = {}
    target_path = f"/deal_block/{start_block}-{end_block}.pkl"
    logger.info(f"target_path2: {target_path}")

    with multiprocessing.Pool(64) as p:
        results = p.map(partial(deal_one_block, _bitcoin_node=bitcoin_node), [block_height for block_height in range(start_block, end_block + 1)])
        for block_height, block_table in results:
            deal_table[block_height] = block_table

    save_hash_table(deal_table, target_path)  # 假设save_hash_table是保存字典的函数


if __name__ == "__main__":
    start_height_str = os.getenv('DEAL_START', '1')
    end_height_str = os.getenv('DEAL_END', '10000')
    start_height = int(start_height_str)
    end_height = int(end_height_str)

    interval = 10000
    bitcoin_node = BitcoinNode()

    # 确保起始块在间隔范围内
    current_block = start_height
    while current_block <= end_height:
        deal(bitcoin_node, current_block, min(current_block + interval, end_height))  # 确保不超过end_block
        current_block += interval

