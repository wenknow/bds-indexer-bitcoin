from pydantic import BaseModel
from typing import Optional

class Challenge(BaseModel):
    model_type: str
    in_total_amount: Optional[float] = None
    out_total_amount: Optional[float] = None
    tx_id_last_4_chars: Optional[str] = None
    block_height: Optional[int] = None

    class Config:
        arbitrary_types_allowed = True