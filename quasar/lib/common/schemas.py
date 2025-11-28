"""
Common Pydantic schemas for API request/response models.
"""
from typing import Optional
from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """Standard error response format."""
    error: str
    details: Optional[str] = None


class SuccessResponse(BaseModel):
    """Standard success response format."""
    status: str
    message: Optional[str] = None

