from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Mapping, Sequence, Optional, Type, Union
from abc import ABC, abstractmethod
import pandas as pd
from pydantic import BaseModel, Field, model_validator

from quasar.lib.providers.core import Bar


class StrategyConfig(BaseModel):
    """
    Base configuration class for all strategies using Pydantic.
    
    Provides helper methods for easy field definition with mandatory
    title and description. Subclass this to define strategy-specific configs.
    
    Example:
        class MyStrategyConfig(StrategyConfig):
            fast_window: int = StrategyConfig.int_field(
                default=20,
                min_value=1,
                max_value=200,
                title="Fast Moving Average Window",
                description="Number of periods for fast moving average"
            )
    """
    
    model_config = {
        "frozen": False,  # Allow runtime reconfiguration
        "validate_assignment": True,
        "extra": "forbid",
    }
    
    @model_validator(mode='after')
    def validate_option_fields(self) -> "StrategyConfig":
        """
        Validate that any fields with enum constraints have valid values.
        
        This ensures option_field() values are validated at runtime.
        """
        # Get the model's field info from the class (not instance)
        for field_name, field_info in self.__class__.model_fields.items():
            # Check if this field has enum constraints in json_schema_extra
            # In Pydantic v2, json_schema_extra is stored in the field's json_schema_extra attribute
            json_schema_extra = getattr(field_info, 'json_schema_extra', None)
            
            # Also check the field's serialization_alias or other metadata
            if json_schema_extra is None:
                # Try to get it from the annotation if it's stored there
                # For now, we'll rely on the Field's json_schema_extra being set
                continue
            
            if isinstance(json_schema_extra, dict) and "enum" in json_schema_extra:
                enum_values = json_schema_extra["enum"]
                field_value = getattr(self, field_name, None)
                
                # Skip if field has default Ellipsis (required field that wasn't provided)
                if field_value is ...:
                    continue
                
                # Skip None values (for optional fields with defaults)
                if field_value is None:
                    # Check if field is optional (has a default)
                    if field_info.default is not ... and field_info.default is not None:
                        continue
                
                # Validate the value is in the enum
                if field_value not in enum_values:
                    raise ValueError(
                        f"Field '{field_name}' must be one of {enum_values}, got '{field_value}'"
                    )
        
        return self
    
    @staticmethod
    def int_field(
        default: int = ...,
        *,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        title: str,
        description: str,
        **kwargs: Any
    ) -> Any:
        """
        Helper to create an integer field with validation and metadata.
        
        Args:
            default: Default value (use ... for required fields)
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            title: Display title (required)
            description: Field description (required)
            **kwargs: Additional Pydantic Field arguments
            
        Returns:
            Field annotation with validation and metadata
        """
        field_kwargs: Dict[str, Any] = {
            "title": title,
            "description": description,
            **kwargs
        }
        
        if default is not ...:
            field_kwargs["default"] = default
        
        if min_value is not None:
            field_kwargs["ge"] = min_value
        if max_value is not None:
            field_kwargs["le"] = max_value
        
        return Field(**field_kwargs)
    
    @staticmethod
    def float_field(
        default: float = ...,
        *,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        title: str,
        description: str,
        **kwargs: Any
    ) -> Any:
        """
        Helper to create a float field with validation and metadata.
        
        Args:
            default: Default value (use ... for required fields)
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            title: Display title (required)
            description: Field description (required)
            **kwargs: Additional Pydantic Field arguments
            
        Returns:
            Field annotation with validation and metadata
        """
        field_kwargs: Dict[str, Any] = {
            "title": title,
            "description": description,
            **kwargs
        }
        
        if default is not ...:
            field_kwargs["default"] = default
        
        if min_value is not None:
            field_kwargs["ge"] = min_value
        if max_value is not None:
            field_kwargs["le"] = max_value
        
        return Field(**field_kwargs)
    
    @staticmethod
    def str_field(
        default: str = ...,
        *,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
        title: str,
        description: str,
        **kwargs: Any
    ) -> Any:
        """
        Helper to create a string field with validation and metadata.
        
        Args:
            default: Default value (use ... for required fields)
            min_length: Minimum string length
            max_length: Maximum string length
            pattern: Regex pattern for validation
            title: Display title (required)
            description: Field description (required)
            **kwargs: Additional Pydantic Field arguments
            
        Returns:
            Field annotation with validation and metadata
        """
        field_kwargs: Dict[str, Any] = {
            "title": title,
            "description": description,
            **kwargs
        }
        
        if default is not ...:
            field_kwargs["default"] = default
        
        if min_length is not None:
            field_kwargs["min_length"] = min_length
        if max_length is not None:
            field_kwargs["max_length"] = max_length
        if pattern is not None:
            field_kwargs["pattern"] = pattern
        
        return Field(**field_kwargs)
    
    @staticmethod
    def bool_field(
        default: bool = False,
        *,
        title: str,
        description: str,
        **kwargs: Any
    ) -> Any:
        """
        Helper to create a boolean field with metadata.
        
        Args:
            default: Default value
            title: Display title (required)
            description: Field description (required)
            **kwargs: Additional Pydantic Field arguments
            
        Returns:
            Field annotation with validation and metadata
        """
        return Field(
            default=default,
            title=title,
            description=description,
            **kwargs
        )
    
    @staticmethod
    def option_field(
        options: List[str],
        default: str = ...,
        *,
        title: str,
        description: str,
        **kwargs: Any
    ) -> Any:
        """
        Helper to create a string field with pre-set option values (enum/dropdown).
        
        Args:
            options: List of allowed string values
            default: Default value (must be one of the options, use ... for required)
            title: Display title (required)
            description: Field description (required)
            **kwargs: Additional Pydantic Field arguments
            
        Returns:
            Field annotation with validation and metadata
            
        Raises:
            ValueError: If default value is not in options list
            
        Example:
            strategy_type: str = StrategyConfig.option_field(
                options=["long", "short", "both"],
                default="long",
                title="Strategy Type",
                description="Whether to trade long, short, or both positions"
            )
        """
        if not options:
            raise ValueError("options list cannot be empty")
        
        if default is not ... and default not in options:
            raise ValueError(f"default value '{default}' must be one of the options: {options}")
        
        # Create field with enum constraint in JSON Schema
        field_kwargs: Dict[str, Any] = {
            "title": title,
            "description": description,
            "json_schema_extra": {
                "enum": options
            },
            **kwargs
        }
        
        if default is not ...:
            field_kwargs["default"] = default
        
        return Field(**field_kwargs)
    
    @classmethod
    def get_json_schema(cls) -> Dict[str, Any]:
        """
        Get JSON Schema representation of this config class.
        
        This can be used by the frontend to generate dynamic forms.
        
        Returns:
            JSON Schema dict
        """
        return cls.model_json_schema(mode='serialization')
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StrategyConfig":
        """
        Create a config instance from a dictionary.
        
        Args:
            data: Dictionary containing config values
            
        Returns:
            Validated StrategyConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert config instance to dictionary.
        
        Returns:
            Dictionary representation of config
        """
        return self.model_dump()


@dataclass(slots=True)
class TargetPosition:
    """Desired net position for a symbol on a given broker account."""

    common_symbol: str
    broker_account: str
    quantity: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderIntent:
    """Explicit order instructions emitted by a strategy."""

    broker_account: str
    common_symbol: str
    side: str  # BUY or SELL
    quantity: float
    order_type: str = "market"
    limit_price: float | None = None
    time_in_force: str = "day"
    tags: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyResult:
    """Container for strategy outputs."""

    targets: List[TargetPosition] = field(default_factory=list)
    orders: List[OrderIntent] = field(default_factory=list)
    notes: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def empty(cls, reason: str | None = None) -> "StrategyResult":
        notes: Dict[str, Any] = {}
        if reason:
            notes["reason"] = reason
        return cls(notes=notes)


class PriceData(dict):
    """
    Price data class that strategies can act on.

    Subclass of dict that exposes helper methods to get price data series.
    Stores OHLCV dataframes keyed by symbol.

    Example:
        # From DataFrames
        price_data = PriceData({
            'BTCUSD': df_btc,  # DataFrame with columns: o, h, l, c, v (ts as index)
            'ETHUSD': df_eth
        })
        
        # From Bar sequences (convenient for database queries)
        price_data = PriceData.from_bars({
            'BTCUSD': [bar1, bar2, ...],  # List of Bar objects
            'ETHUSD': [bar1, bar2, ...]
        })
        
        # Usage
        closes = price_data.get_close()  # DataFrame with columns: BTCUSD, ETHUSD
        btc_close = price_data.get_close('BTCUSD')  # Series with BTC closing prices
    """

    def __init__(self, data: Mapping[str, pd.DataFrame] | None = None):
        """
        Initialize PriceData with a dictionary of symbol -> OHLCV dataframe mappings.

        Args:
            data: Dictionary mapping symbols to pandas DataFrames with OHLCV columns.
                  DataFrames should have 'ts' as index and columns: o, h, l, c, v
        """
        if data is None:
            data = {}
        # Ensure all DataFrames are sorted by index (timestamp)
        sorted_data = {}
        for symbol, df in data.items():
            if not df.empty:
                sorted_data[symbol] = df.sort_index()
            else:
                sorted_data[symbol] = df
        dict.__init__(self, sorted_data)

    @classmethod
    def from_bars(
        cls,
        data: Mapping[str, Sequence[Bar]]
    ) -> "PriceData":
        """
        Create PriceData from a mapping of symbols to Bar sequences.

        This is the most convenient method when pulling data from the database

        Args:
            data: Dictionary mapping symbols to sequences of Bar objects.
                  Each Bar should have: ts, sym, o, h, l, c, v

        Returns:
            PriceData instance with DataFrames keyed by symbol

        Example:
            bars_by_symbol = {
                'BTCUSD': [bar1, bar2, bar3],
                'ETHUSD': [bar1, bar2, bar3]
            }
            price_data = PriceData.from_bars(bars_by_symbol)
        """
        dataframes: Dict[str, pd.DataFrame] = {}

        for symbol, bars in data.items():
            if not bars:
                # Empty sequence - create empty DataFrame with correct structure
                dataframes[symbol] = pd.DataFrame(
                    columns=['o', 'h', 'l', 'c', 'v'],
                    dtype=float
                )
                continue

            # Convert Bar sequence to DataFrame
            rows = []
            for bar in bars:
                rows.append({
                    'ts': bar['ts'],
                    'o': bar['o'],
                    'h': bar['h'],
                    'l': bar['l'],
                    'c': bar['c'],
                    'v': bar['v']
                })

            df = pd.DataFrame(rows)
            # Set ts as index
            df.set_index('ts', inplace=True)
            # Sort by index (timestamp) to ensure chronological order
            df.sort_index(inplace=True)
            # Ensure columns are in correct order
            df = df[['o', 'h', 'l', 'c', 'v']]
            dataframes[symbol] = df

        return cls(dataframes)

    def _get_price_column(
        self, column: str, symbol: Optional[str] = None
    ) -> pd.DataFrame | pd.Series:
        """
        Core method to extract a price column (o, h, l, c, v) from price data.

        Args:
            column: The column name to extract ('o', 'h', 'l', 'c', or 'v')
            symbol: Optional symbol name. If provided, returns data for that symbol only.
                    If None, returns a DataFrame with all symbols as columns.

        Returns:
            - If symbol is provided: pd.Series with the price column for that symbol
            - If symbol is None: pd.DataFrame with symbols as columns and the price column as values

        Raises:
            KeyError: If symbol is provided but not found in the data
            KeyError: If the requested column doesn't exist in the dataframe
        """
        if symbol is not None:
            # Return single symbol data
            if symbol not in self:
                raise KeyError(f"Symbol '{symbol}' not found in PriceData")
            df = self[symbol]
            if column not in df.columns:
                raise KeyError(
                    f"Column '{column}' not found in dataframe for symbol '{symbol}'. "
                    f"Available columns: {list(df.columns)}"
                )
            return df[column]

        # Return all symbols as columns
        if not self:
            return pd.DataFrame()

        # Collect series for each symbol
        series_dict: Dict[str, pd.Series] = {}
        for sym, df in self.items():
            if column not in df.columns:
                continue  # Skip symbols that don't have this column
            series_dict[sym] = df[column]

        if not series_dict:
            return pd.DataFrame()

        # Combine all series into a DataFrame, aligning on index (timestamp)
        result = pd.DataFrame(series_dict)
        # Ensure result is sorted by index (timestamp)
        result.sort_index(inplace=True)
        return result

    def get_open(self, symbol: Optional[str] = None) -> pd.DataFrame | pd.Series:
        """
        Get open prices.

        Args:
            symbol: Optional symbol name. If provided, returns Series for that symbol.
                   If None, returns DataFrame with all symbols as columns.

        Returns:
            pd.Series if symbol is provided, pd.DataFrame otherwise
        """
        return self._get_price_column("o", symbol)

    def get_high(self, symbol: Optional[str] = None) -> pd.DataFrame | pd.Series:
        """
        Get high prices.

        Args:
            symbol: Optional symbol name. If provided, returns Series for that symbol.
                   If None, returns DataFrame with all symbols as columns.

        Returns:
            pd.Series if symbol is provided, pd.DataFrame otherwise
        """
        return self._get_price_column("h", symbol)

    def get_low(self, symbol: Optional[str] = None) -> pd.DataFrame | pd.Series:
        """
        Get low prices.

        Args:
            symbol: Optional symbol name. If provided, returns Series for that symbol.
                   If None, returns DataFrame with all symbols as columns.

        Returns:
            pd.Series if symbol is provided, pd.DataFrame otherwise
        """
        return self._get_price_column("l", symbol)

    def get_close(self, symbol: Optional[str] = None) -> pd.DataFrame | pd.Series:
        """
        Get closing prices.

        Args:
            symbol: Optional symbol name. If provided, returns Series for that symbol.
                   If None, returns DataFrame with all symbols as columns.

        Returns:
            pd.Series if symbol is provided, pd.DataFrame otherwise
        """
        return self._get_price_column("c", symbol)

    def get_volume(self, symbol: Optional[str] = None) -> pd.DataFrame | pd.Series:
        """
        Get volume data.

        Args:
            symbol: Optional symbol name. If provided, returns Series for that symbol.
                   If None, returns DataFrame with all symbols as columns.

        Returns:
            pd.Series if symbol is provided, pd.DataFrame otherwise
        """
        return self._get_price_column("v", symbol)



@dataclass(slots=True)
class StrategyContext:
    """
    Snapshot of runtime state that strategies can act on.

    This intentionally mirrors the minimal data we expect to wire up when the
    StrategyEngine is fully implemented.
    """

    clock: datetime
    interval: str
    prices: PriceData
    positions: Mapping[str, Any]  # placeholder for broker-native positions
    cash: Mapping[str, float]
    # Raw config values for this execution; already validated upstream
    config: Mapping[str, Any] = field(default_factory=dict)
    policies: Mapping[str, Any] = field(default_factory=dict)


class BaseStrategy(ABC):
    """
    Base class all user strategies must inherit from.

    Strategies define a ConfigModel (schema) and implement _on_bar(). The public
    on_bar() injects config from the StrategyContext at runtime to keep instances
    stateless while preserving type-safe access via self.config.
    """

    ConfigModel: Type[StrategyConfig] = StrategyConfig

    @property
    @abstractmethod
    def name(self) -> str:  # unique strategy id
        ...

    @property
    @abstractmethod
    def lookback(self) -> int:
        """Number of bars required for the strategy to operate."""
        ...

    def __init__(self, config: Optional[Union[Dict[str, Any], StrategyConfig]] = None):
        """
        Optional initialization. Creates a default config instance so self.config
        is always available. Runtime values are injected per bar.

        Args:
            config: Optional config dict or StrategyConfig instance.
        """
        if config is None:
            self.config: StrategyConfig = self.ConfigModel()
        elif isinstance(config, dict):
            self.config = self.ConfigModel(**config)
        elif isinstance(config, StrategyConfig):
            if not isinstance(config, self.ConfigModel):
                self.config = self.ConfigModel(**config.model_dump())
            else:
                self.config = config
        else:
            raise TypeError(
                f"config must be None, dict, or StrategyConfig, got {type(config)}"
            )

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        """
        Get JSON Schema for this strategy's configuration.
        Used by the frontend to generate dynamic forms.
        """
        return cls.ConfigModel.get_json_schema()

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a configuration dictionary against this strategy's schema.

        Returns validated dictionary (with defaults applied).
        Raises ValidationError on failure.
        """
        validated = cls.ConfigModel(**config)
        return validated.model_dump()

    async def on_bar(self, context: StrategyContext) -> StrategyResult:
        """
        Public entry point. Injects config from context (already validated) and
        then delegates to the strategy implementation.
        """
        self.config = self.ConfigModel.model_construct(**context.config)
        return await self._on_bar(context)

    @abstractmethod
    async def _on_bar(self, context: StrategyContext) -> StrategyResult:
        """
        Strategy implementation. Override this method in subclasses.

        Use self.config for typed access to configuration values.
        """

    async def on_start(self, context: StrategyContext) -> None:
        """Optional hook invoked when a strategy instance is bootstrapped."""

    async def on_stop(self, context: StrategyContext) -> None:
        """Optional hook invoked when a strategy instance is shut down."""

