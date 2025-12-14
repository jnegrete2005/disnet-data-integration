from typing import Optional
from pydantic import BaseModel, Field


class DrugCombData(BaseModel):
    id: int
    drug_combination: str = Field(..., alias="drugCombination")
    drug1: str
    drug2: str
    cell_line: str = Field(..., alias="cellName")
    source: str

    hsa: Optional[float] = Field(None, alias="HSA")
    bliss: Optional[float] = Field(None, alias="Bliss")
    loewe: Optional[float] = Field(None, alias="Loewe")
    zip: Optional[float] = Field(None, alias="ZIP")


class DrugCombAPIResponse(BaseModel):
    code: int
    msg: str
    data: Optional[DrugCombData] = None
