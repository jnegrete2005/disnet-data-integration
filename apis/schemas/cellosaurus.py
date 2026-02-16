from typing import List, Optional
from pydantic import BaseModel, Field, AliasPath, validator


class CellLineEntry(BaseModel):
    # Flattening: Cellosaurus -> cell-line-list -> [0] -> accession-list -> [0] -> value
    cellosaurus_ac: Optional[str] = Field(
        None, validation_alias=AliasPath("accession-list", 0, "value")
    )

    # Flattening: disease-list -> [0] -> accession (This is the NCIt ID)
    ncit_id: Optional[str] = Field(
        None, validation_alias=AliasPath("disease-list", 0, "accession")
    )

    # Flattening: derived-from-site-list -> [0] -> site -> value
    site: Optional[str] = Field(
        None, validation_alias=AliasPath("derived-from-site-list", 0, "site", "value")
    )


class CellosaurusResponse(BaseModel):
    # Wrapper for the top-level "Cellosaurus" -> "cell-line-list"
    cell_lines: List[CellLineEntry] = Field(
        [], validation_alias=AliasPath("Cellosaurus", "cell-line-list")
    )
