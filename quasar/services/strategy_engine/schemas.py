from pydantic import BaseModel, Field


class StrategyValidateRequest(BaseModel):
    """Payload received from the Registry during validation calls."""

    file_path: str = Field(..., description="Absolute path to the uploaded strategy file.")


class StrategyValidateResponse(BaseModel):
    """Minimal response stub returned to the Registry."""

    status: str = Field(..., description="Validation outcome (placeholder).")
    class_name: str = Field(..., description="Strategy class name resolved from file.")
    subclass_type: str = Field(..., description="Future hook for strategy subtype metadata.")
    details: str | None = Field(
        default=None,
        description="Optional human readable message explaining the validation result.",
    )

