from protocol import NETWORK_BITCOIN
from node import BitcoinNode

class NodeFactory:
    @classmethod
    def create_node(cls, network: str):
        node_class = {
            NETWORK_BITCOIN: BitcoinNode
        }.get(network)

        if node_class is None:
            raise ValueError(f"Unsupported network: {network}")

        return node_class()