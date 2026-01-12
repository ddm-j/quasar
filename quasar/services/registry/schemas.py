"""
Registry-specific Pydantic schemas for API request/response models.
"""
from typing import Optional, List, Literal, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field

from quasar.lib.enums import AssetClass


# Path parameter types
ClassType = Literal["provider", "broker"]


# File Upload Response
class FileUploadResponse(BaseModel):
    """Response model for file upload endpoint."""
    status: str = Field(..., description="Upload status message")


# Asset Update Response
class UpdateAssetsResponse(BaseModel):
    """Response model for asset update endpoints."""
    class_name: str
    class_type: str
    total_symbols: int = 0
    processed_symbols: int = 0
    added_symbols: int = 0
    updated_symbols: int = 0
    failed_symbols: int = 0
    identity_matched: int = 0      # Assets identified by matcher
    identity_skipped: int = 0      # Assets skipped (already had primary_id)
    mappings_created: int = 0      # Automated mappings created
    mappings_skipped: int = 0      # Automated mappings skipped (already exist)
    mappings_failed: int = 0       # Automated mappings that failed to create
    # Index membership statistics (only populated for IndexProviders)
    members_added: int = 0
    members_removed: int = 0
    members_unchanged: int = 0
    status: int = 200
    error: Optional[str] = None
    message: Optional[str] = None


# Class Summary Response
class ClassSummaryItem(BaseModel):
    """Single class summary item."""
    id: int
    class_name: str
    class_type: str
    class_subtype: str
    uploaded_at: str
    asset_count: int


# Delete Class Response
class DeleteClassResponse(BaseModel):
    """Response model for delete class endpoint."""
    message: str
    class_name: str
    class_type: str
    file_deleted: bool


# Asset Query Parameters
class AssetQueryParams(BaseModel):
    """Query parameters for GET /internal/assets endpoint."""
    limit: int = Field(default=25, ge=1, le=100, description="Number of items per page")
    offset: int = Field(default=0, ge=0, description="Starting index")
    sort_by: str = Field(default="class_name,symbol", description="Column(s) to sort by, comma-separated")
    sort_order: str = Field(default="asc", description="Sort order ('asc' or 'desc'), comma-separated if multiple sort_by")
    class_name_like: Optional[str] = Field(default=None, description="Partial match for class_name")
    class_type: Optional[str] = Field(default=None, description="Exact match for class_type ('provider' or 'broker')")
    asset_class: Optional[AssetClass] = Field(default=None, description="Exact match for asset_class")
    base_currency_like: Optional[str] = Field(default=None, description="Partial match for base_currency")
    quote_currency_like: Optional[str] = Field(default=None, description="Partial match for quote_currency")
    country_like: Optional[str] = Field(default=None, description="Partial match for country")
    symbol_like: Optional[str] = Field(default=None, description="Partial match for symbol")
    name_like: Optional[str] = Field(default=None, description="Partial match for name")
    exchange_like: Optional[str] = Field(default=None, description="Partial match for exchange")

    # New identity field filters
    primary_id_like: Optional[str] = Field(default=None, description="Partial match for primary_id")
    primary_id_source: Optional[str] = Field(default=None, description="Exact match: 'provider', 'matcher', 'manual'")
    matcher_symbol_like: Optional[str] = Field(default=None, description="Partial match for matcher_symbol")
    identity_match_type: Optional[str] = Field(default=None, description="Exact match: 'exact_alias', 'fuzzy_symbol'")
    asset_class_group: Optional[str] = Field(default=None, description="Exact match: 'securities', 'crypto'")


# Common Symbol Query Parameters
class CommonSymbolQueryParams(BaseModel):
    """Query parameters for GET /api/registry/common-symbols endpoint."""
    limit: int = Field(default=25, ge=1, le=100, description="Number of items per page")
    offset: int = Field(default=0, ge=0, description="Starting index")
    sort_by: str = Field(default="common_symbol", description="Column(s) to sort by, comma-separated")
    sort_order: str = Field(default="asc", description="Sort order ('asc' or 'desc'), comma-separated if multiple sort_by")
    common_symbol_like: Optional[str] = Field(default=None, description="Partial match for common_symbol")


# Asset Mapping Query Parameters
class AssetMappingQueryParams(BaseModel):
    """Query parameters for GET /api/registry/asset-mappings endpoint."""
    limit: int = Field(default=25, ge=1, le=100, description="Number of items per page")
    offset: int = Field(default=0, ge=0, description="Starting index")
    sort_by: str = Field(default="class_name,common_symbol", description="Column(s) to sort by, comma-separated")
    sort_order: str = Field(default="asc", description="Sort order ('asc' or 'desc'), comma-separated if multiple sort_by")
    # Filter parameters (matching current endpoint signature)
    common_symbol: Optional[str] = Field(default=None, description="Exact match for common_symbol")
    common_symbol_like: Optional[str] = Field(default=None, description="Partial match for common_symbol")
    class_name: Optional[str] = Field(default=None, description="Exact match for class_name")
    class_name_like: Optional[str] = Field(default=None, description="Partial match for class_name")
    class_type: Optional[ClassType] = Field(default=None, description="Exact match for class_type ('provider' or 'broker')")
    class_symbol: Optional[str] = Field(default=None, description="Exact match for class_symbol")
    class_symbol_like: Optional[str] = Field(default=None, description="Partial match for class_symbol")
    is_active: Optional[bool] = Field(default=None, description="Exact match for is_active")


# Asset Item
class AssetItem(BaseModel):
    """Single asset item."""
    id: int
    class_name: str
    class_type: str
    external_id: Optional[str] = None
    primary_id: Optional[str] = None
    primary_id_source: Optional[str] = None  # NEW
    symbol: str
    matcher_symbol: Optional[str] = None  # NEW
    name: Optional[str] = None
    exchange: Optional[str] = None
    asset_class: Optional[AssetClass] = None
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    country: Optional[str] = None
    # Identity matching fields
    identity_conf: Optional[float] = None  # NEW
    identity_match_type: Optional[str] = None  # NEW
    identity_updated_at: Optional[datetime] = None  # NEW
    # Generated columns
    asset_class_group: Optional[str] = None  # NEW
    sym_norm_full: Optional[str] = None  # NEW
    sym_norm_root: Optional[str] = None  # NEW


# Common Symbol Item
class CommonSymbolItem(BaseModel):
    """Single common symbol item with provider count."""
    common_symbol: str
    provider_count: int


# Asset Response
class AssetResponse(BaseModel):
    """Response model for GET /internal/assets endpoint."""
    items: List[AssetItem]
    total_items: int
    limit: int
    offset: int
    page: int
    total_pages: int


# Common Symbol Response
class CommonSymbolResponse(BaseModel):
    """Response model for GET /api/registry/common-symbols endpoint."""
    items: List[CommonSymbolItem]
    total_items: int
    limit: int
    offset: int
    page: int
    total_pages: int


# Asset Mapping Create
class AssetMappingCreate(BaseModel):
    """Request model for creating asset mapping."""
    common_symbol: str = Field(..., description="Common symbol identifier")
    class_name: str = Field(..., description="Class name (provider/broker name)")
    class_type: ClassType = Field(..., description="Class type: 'provider' or 'broker'")
    class_symbol: str = Field(..., description="Class-specific symbol")
    is_active: bool = Field(default=True, description="Whether the mapping is active")


# Asset Mapping Response
class AssetMappingResponse(BaseModel):
    """Response model for asset mapping endpoints."""
    common_symbol: str
    class_name: str
    class_type: str
    class_symbol: str
    is_active: bool
    primary_id: Optional[str] = None
    asset_class: Optional[str] = None  # Changed from AssetClass to str for simplicity


# Asset Mapping Paginated Response
class AssetMappingPaginatedResponse(BaseModel):
    """Response model for GET /api/registry/asset-mappings endpoint (paginated)."""
    items: List[AssetMappingResponse]
    total_items: int
    limit: int
    offset: int
    page: int
    total_pages: int


# Asset Mapping Create/Response (batch-capable)
# Requests accept a single object or a list for backward compatibility.
# Responses are always a list for clarity and OpenAPI friendliness.
AssetMappingCreateRequest = Union[AssetMappingCreate, List[AssetMappingCreate]]
AssetMappingCreateResponse = List[AssetMappingResponse]


# Asset Mapping Update
class AssetMappingUpdate(BaseModel):
    """Request model for updating asset mapping (partial update)."""
    common_symbol: Optional[str] = Field(default=None, description="Common symbol identifier")
    is_active: Optional[bool] = Field(default=None, description="Whether the mapping is active")


# Asset Mapping Suggestions
class SuggestionItem(BaseModel):
    """Single suggested mapping candidate."""

    source_class: str
    source_type: str
    source_symbol: str
    source_name: Optional[str] = None

    target_class: str
    target_type: str
    target_symbol: str
    target_name: Optional[str] = None
    target_common_symbol: Optional[str] = Field(
        default=None,
        description="Existing common_symbol for target if already mapped"
    )

    proposed_common_symbol: str
    score: float
    id_match: bool
    external_id_match: bool
    norm_match: bool
    base_quote_match: bool
    exchange_match: bool
    sym_root_similarity: float = 0.0
    name_similarity: float
    target_already_mapped: bool


class SuggestionsResponse(BaseModel):
    """Response payload for suggestions endpoint with cursor-based pagination.

    Cursor pagination provides consistent, efficient paging through large result sets.
    Use `next_cursor` for subsequent requests instead of incrementing offset.
    """

    items: List[SuggestionItem]
    total: Optional[int] = None  # Only returned when include_total=true
    limit: int
    offset: int = 0  # Deprecated: kept for backwards compatibility
    next_cursor: Optional[str] = None  # Opaque cursor for next page
    has_more: bool = False  # True if more results available


# Provider Configuration Schemas
class CryptoPreferences(BaseModel):
    """Crypto-specific trading preferences."""
    preferred_quote_currency: Optional[str] = Field(
        default=None,
        description="Preferred quote currency for crypto pairs (e.g., USDC, USDT, USD)"
    )


class ProviderPreferences(BaseModel):
    """Provider configuration preferences."""
    crypto: Optional[CryptoPreferences] = Field(default=None)


class ProviderPreferencesResponse(BaseModel):
    """Response model for provider preferences endpoint."""
    class_name: str
    class_type: str
    preferences: ProviderPreferences


class ProviderPreferencesUpdate(BaseModel):
    """Request model for updating provider preferences (partial update)."""
    crypto: Optional[CryptoPreferences] = Field(default=None)


class AvailableQuoteCurrenciesResponse(BaseModel):
    """Response model for available quote currencies endpoint."""
    class_name: str
    class_type: str
    available_quote_currencies: List[str]


# =============================================================================
# Index Management Schemas
# =============================================================================

# Index Query Parameters
class IndexQueryParams(BaseModel):
    """Query parameters for GET /api/registry/indices endpoint."""
    limit: int = Field(default=25, ge=1, le=100, description="Number of items per page")
    offset: int = Field(default=0, ge=0, description="Starting index")
    index_type: Optional[Literal["IndexProvider", "UserIndex"]] = Field(
        default=None, description="Filter by index type"
    )
    sort_by: str = Field(default="class_name", description="Column to sort by")
    sort_order: str = Field(default="asc", description="Sort order ('asc' or 'desc')")


class IndexMemberQueryParams(BaseModel):
    """Query parameters for GET /api/registry/indices/{name}/members endpoint."""
    limit: int = Field(default=100, ge=1, le=500, description="Number of items per page")
    offset: int = Field(default=0, ge=0, description="Starting index")
    as_of: Optional[datetime] = Field(
        default=None, description="Point-in-time query (ISO 8601 timestamp)"
    )
    sort_by: str = Field(default="weight", description="Column to sort by")
    sort_order: str = Field(default="desc", description="Sort order ('asc' or 'desc')")


# Index Request Models
class UserIndexCreate(BaseModel):
    """Request model for creating a UserIndex."""
    name: str = Field(..., min_length=1, max_length=100, description="Index name (unique)")
    description: Optional[str] = Field(default=None, description="Index description")


class UserIndexMemberCreate(BaseModel):
    """Single member for UserIndex."""
    common_symbol: str = Field(..., description="Common symbol to add to index")
    weight: Optional[float] = Field(default=None, gt=0, description="Weight (optional, must be positive)")


class UserIndexMembersUpdate(BaseModel):
    """Request model for updating UserIndex members (full replacement)."""
    members: List[UserIndexMemberCreate] = Field(..., description="List of members")


class IndexConstituentSync(BaseModel):
    """Single constituent from IndexProvider for sync endpoint."""
    symbol: str = Field(..., description="Provider symbol")
    weight: Optional[float] = Field(default=None, description="Weight in index")
    name: Optional[str] = Field(default=None, description="Asset name")
    asset_class: Optional[str] = Field(default=None, description="Asset class")
    matcher_symbol: Optional[str] = Field(default=None, description="Symbol for matching")
    base_currency: Optional[str] = Field(default=None, description="Base currency")
    quote_currency: Optional[str] = Field(default=None, description="Quote currency")
    exchange: Optional[str] = Field(default=None, description="Exchange identifier")


class IndexSyncRequest(BaseModel):
    """Request body for sync endpoint (from DataHub)."""
    constituents: List[IndexConstituentSync] = Field(..., description="List of constituents")


# Index Response Models
class IndexItem(BaseModel):
    """Single index item for list response."""
    class_name: str
    class_type: str
    index_type: str  # 'IndexProvider' or 'UserIndex'
    uploaded_at: Optional[datetime] = None
    current_member_count: int
    preferences: Optional[Dict[str, Any]] = None


class IndexMemberItem(BaseModel):
    """Single index member item."""
    id: int
    asset_class_name: Optional[str] = None
    asset_class_type: Optional[str] = None
    asset_symbol: Optional[str] = None
    common_symbol: Optional[str] = None
    effective_symbol: str  # Computed: asset_symbol or common_symbol
    weight: Optional[float] = None
    valid_from: datetime
    source: str


class IndexDetailResponse(BaseModel):
    """Response model for GET /api/registry/indices/{name} endpoint."""
    index: IndexItem
    members: List[IndexMemberItem]


class IndexListResponse(BaseModel):
    """Response model for GET /api/registry/indices endpoint (paginated)."""
    items: List[IndexItem]
    total_items: int
    limit: int
    offset: int
    page: int
    total_pages: int


class IndexMembersResponse(BaseModel):
    """Response model for GET /api/registry/indices/{name}/members endpoint (paginated)."""
    items: List[IndexMemberItem]
    total_items: int
    limit: int
    offset: int
    page: int
    total_pages: int


class IndexSyncResponse(BaseModel):
    """Response model for POST /api/registry/indices/{name}/sync endpoint."""
    index_class_name: str
    assets_created: int
    assets_updated: int
    members_added: int
    members_removed: int
    members_unchanged: int
