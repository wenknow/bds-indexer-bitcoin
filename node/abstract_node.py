from abc import ABC, abstractmethod


class Node(ABC):
    def __init__(self):
       pass

    @abstractmethod
    def get_current_block_height(self):
        ...

    @abstractmethod
    def get_block_by_height(self, block_height):
        ...