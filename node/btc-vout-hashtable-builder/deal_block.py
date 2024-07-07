from node.node import BitcoinNode
from node.node_utils import parse_block_data
from utils import save_hash_table
import concurrent.futures
from threading import Lock
from setup_logger import setup_logger

logger = setup_logger("Indexer")


def parse_args():
    parser = argparse.ArgumentParser(description='Construct a hash table from vout csv data.')

    parser.add_argument('--start', type=str, help='Path to the CSV file')
    parser.add_argument('--end', type=str, help='Path to the target pickle file')
    args = parser.parse_args()

    return args.start, args.end


def deal_one_block_multithreaded(_bitcoin_node, block_data, lock):
    block_table = {}
    transactions = block_data.transactions
    logger.info(f"start deal block: {block_data.block_height}")

    def process_transaction(tx):
        nonlocal block_table
        in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, out_total_amount = _bitcoin_node.process_in_memory_txn_for_indexing(tx)
        with lock:
            block_table[tx] = {
                'in_amount_by_address': in_amount_by_address,
                'out_amount_by_address': out_amount_by_address,
                'input_addresses': input_addresses,
                'output_addresses': output_addresses,
                'in_total_amount': in_total_amount,
                'out_total_amount': out_total_amount
            }

    # 设置内层线程池的大小，根据具体情况调整，避免过度使用CPU资源
    num_inner_threads = 32  # 假设一个区块内最多32个交易并行处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_inner_threads) as executor:
        futures = {executor.submit(process_transaction, tx) for tx in transactions}
        for future in concurrent.futures.as_completed(futures):
            pass

    logger.info(f"success deal block: {block_data.block_height}")
    return block_table


def main():
    start_block, end_block = parse_args()
    if not start_block or not end_block:
        logger.error("Provide start_block end_block and target_path parameter.")
        exit()

    deal_table = {}
    target_path = f"/deal_block/{start_block}-{end_block}.pkl"
    logger.info(f"target_path: {target_path}")
    bitcoin_node = BitcoinNode()
    lock = Lock()  # 用于保护共享资源block_table的线程锁

    # 设置外层线程池的大小，合理分配CPU核心
    num_outer_threads = 16  # 假设最多同时处理16个区块
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_outer_threads) as executor:
        future_to_block = {
            executor.submit(lambda block_height: (block_height, deal_one_block_multithreaded(bitcoin_node, parse_block_data(bitcoin_node.get_block_by_height(block_height)), lock)), block_height): block_height
            for block_height in range(start_block, end_block + 1)
        }

        for future in concurrent.futures.as_completed(future_to_block):
            block_height, block_table = future.result()
            deal_table[block_height] = block_table

    save_hash_table(deal_table, target_path)  # 假设save_hash_table是保存字典的函数


if __name__ == "__main__":
    main()

