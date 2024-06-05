from models.balance_tracking.balance_search import BalanceSearch

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    balance_indexer = BalanceSearch()
    
    print("Executing sql query...")
    indexed_block_height_ranges = balance_indexer.find_indexed_block_height_ranges()
    print(f"Found indexed block height ranges: {indexed_block_height_ranges}")
