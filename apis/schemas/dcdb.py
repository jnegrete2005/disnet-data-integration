from typing import Optional, TypeVar, Generic
from pydantic import BaseModel, Field

T = TypeVar('T')


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


class DrugData(BaseModel):
    c_ids: str = Field(..., alias="cIds")
    drug_name_official: str = Field(..., alias="drugNameOfficial")
    smiles_string: Optional[str] = Field(None, alias="smilesString")


class DrugCombDBAPIResponse(BaseModel, Generic[T]):
    code: int
    msg: str
    data: Optional[T] = None
