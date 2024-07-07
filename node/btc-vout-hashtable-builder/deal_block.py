import os
from node.node import BitcoinNode
from node.node_utils import parse_block_data
from utils import save_hash_table
from setup_logger import setup_logger
from dotenv import load_dotenv
import concurrent.futures
import time


load_dotenv()
logger = setup_logger("Indexer")


def get_block_with_retry(bitcoin_node, block_height, retries=30, delay=2):
    for attempt in range(retries):
        res = bitcoin_node.get_block_by_height(block_height)
        if res is not None:
            return res
        logger.error(f"Error getting block {block_height}, attempt {attempt + 1}/{retries}")
        time.sleep(delay)


def deal_one_block(_bitcoin_node, block_data):
    block_table = {}
    transactions = block_data.transactions
    for tx in transactions:
        in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, out_total_amount = _bitcoin_node.process_in_memory_txn_for_indexing(tx)
        block_table[tx.tx_id] = {
            'in_amount_by_address': in_amount_by_address,
            'out_amount_by_address': out_amount_by_address,
            'input_addresses': input_addresses,
            'output_addresses': output_addresses,
            'in_total_amount': in_total_amount,
            'out_total_amount': out_total_amount
        }
    if block_data.block_height % 100 == 0:
        logger.info(f"success deal block: {block_data.block_height}")
    return block_table


def process_block(_bitcoin_node, block_height):
    block = get_block_with_retry(_bitcoin_node, block_height)
    block_data = parse_block_data(block)
    return deal_one_block(_bitcoin_node, block_data)


def deal(bitcoin_node, start_block, end_block):
    deal_table = {}
    target_path = f"/deal_block/{start_block}-{end_block}.pkl"
    logger.info(f"target_path: {target_path}")

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = [executor.submit(process_block, bitcoin_node, block_height) for block_height in range(start_block, end_block + 1)]

        for future in concurrent.futures.as_completed(results):
            block_height, result = future.result()
            deal_table[block_height] = result

    save_hash_table(deal_table, target_path)


if __name__ == "__main__":
    start_height_str = os.getenv('DEAL_START', '1')
    end_height_str = os.getenv('DEAL_END', '10000')
    start_height = int(start_height_str)
    end_height = int(end_height_str)

    interval = 30000
    bitcoin_node = BitcoinNode()

    # 确保起始块在间隔范围内
    current_block = start_height
    while current_block <= end_height:
        deal(bitcoin_node, current_block, min(current_block + interval - 1, end_height))  # 确保不超过end_block
        current_block += interval

