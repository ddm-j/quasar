"""
DataHub-specific Pydantic schemas for API request/response models.
"""
from typing import Optional, List
from pydantic import BaseModel, Field

from quasar.providers.core import SymbolInfo


class ProviderValidateRequest(BaseModel):
    """Request model for provider validation endpoint."""
    file_path: str = Field(..., description="Path to the provider file to validate")


class ProviderValidateResponse(BaseModel):
    """Response model for provider validation endpoint."""
    status: str = Field(..., description="Validation status")
    class_name: str = Field(..., description="Name of the validated provider class")
    subclass_type: str = Field(..., description="Type of provider: Historical or Live")
    module_name: str = Field(..., description="Name of the module")
    file_path: str = Field(..., description="Path to the validated file")


# Note: AvailableSymbolsResponse is just a List[SymbolInfo]
# We'll use List[dict] in the endpoint return type since SymbolInfo is a TypedDict
# FastAPI can serialize TypedDict directly, but for type hints we use dict
AvailableSymbolsResponse = List[dict]  # List of SymbolInfo (TypedDict)

