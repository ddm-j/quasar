"""
DataHub-specific Pydantic schemas for API request/response models.
"""
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime

from quasar.lib.providers.core import SymbolInfo


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


AvailableSymbolsResponse = List[dict]  # List of SymbolInfo (TypedDict)


# Data Explorer API Schemas
class AssetInfo(BaseModel):
    """Asset metadata information."""
    name: Optional[str] = Field(None, description="Asset name")
    base_currency: Optional[str] = Field(None, description="Base currency")
    quote_currency: Optional[str] = Field(None, description="Quote currency")
    exchange: Optional[str] = Field(None, description="Exchange name")
    asset_class: Optional[str] = Field(None, description="Asset class")


class SymbolSearchItem(BaseModel):
    """Single symbol search result."""
    common_symbol: str = Field(..., description="Common symbol identifier")
    provider: str = Field(..., description="Provider class name")
    provider_symbol: str = Field(..., description="Provider-specific symbol")
    has_historical: bool = Field(..., description="Whether historical data is available")
    has_live: bool = Field(..., description="Whether live data is available")
    available_intervals: List[str] = Field(default_factory=list, description="Available intervals for this symbol")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    asset_info: Optional[AssetInfo] = Field(None, description="Asset metadata")


class SymbolSearchResponse(BaseModel):
    """Response model for symbol search endpoint."""
    items: List[SymbolSearchItem] = Field(..., description="List of search results")
    total: int = Field(..., description="Total number of matching results")
    limit: int = Field(..., description="Limit applied to the query")


class OHLCBar(BaseModel):
    """Single OHLC bar data."""
    time: int = Field(..., description="Unix timestamp in seconds")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Close price")
    volume: float = Field(..., description="Volume")


class OHLCDataResponse(BaseModel):
    """Response model for OHLC data retrieval endpoint."""
    provider: str = Field(..., description="Provider class name")
    symbol: str = Field(..., description="Provider-specific symbol")
    common_symbol: Optional[str] = Field(None, description="Common symbol identifier")
    data_type: str = Field(..., description="Data type: 'historical' or 'live'")
    interval: str = Field(..., description="Interval string")
    bars: List[OHLCBar] = Field(..., description="List of OHLC bars")
    count: int = Field(..., description="Number of bars returned")
    from_time: Optional[datetime] = Field(None, description="Start time of data range")
    to_time: Optional[datetime] = Field(None, description="End time of data range")
    has_more: bool = Field(..., description="Whether more data is available")


class DataTypeInfo(BaseModel):
    """Information about data availability for a specific data type."""
    available: bool = Field(..., description="Whether data is available")
    intervals: List[str] = Field(default_factory=list, description="Available intervals")
    earliest: Optional[datetime] = Field(None, description="Earliest data timestamp")
    latest: Optional[datetime] = Field(None, description="Latest data timestamp")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")


class OtherProvider(BaseModel):
    """Information about another provider for the same common symbol."""
    provider: str = Field(..., description="Provider class name")
    provider_symbol: str = Field(..., description="Provider-specific symbol")
    has_historical: bool = Field(..., description="Whether historical data is available")
    has_live: bool = Field(..., description="Whether live data is available")


class SymbolMetadataResponse(BaseModel):
    """Response model for symbol metadata endpoint."""
    common_symbol: str = Field(..., description="Common symbol identifier")
    provider: str = Field(..., description="Provider class name")
    provider_symbol: str = Field(..., description="Provider-specific symbol")
    data_types: dict[str, DataTypeInfo] = Field(..., description="Data type availability information")
    asset_info: Optional[AssetInfo] = Field(None, description="Asset metadata")
    other_providers: List[OtherProvider] = Field(default_factory=list, description="Other providers for the same common symbol")

