"""Unit tests for Registry utility modules.

Tests pagination cursor encoding/decoding and FilterBuilder query building.
"""

import pytest

from quasar.services.registry.utils.pagination import encode_cursor, decode_cursor
from quasar.services.registry.utils.query_builder import FilterBuilder


class TestPaginationCursors:
    """Tests for cursor encoding and decoding."""

    def test_encode_cursor_produces_valid_base64(self):
        """Encoded cursor should be a valid base64 string."""
        cursor = encode_cursor(0.95, 'AAPL', 'Apple Inc')
        assert isinstance(cursor, str)
        # Should not contain URL-unsafe characters
        assert '+' not in cursor
        assert '/' not in cursor

    def test_decode_cursor_returns_correct_tuple(self):
        """Decoded cursor should return the original values."""
        cursor = encode_cursor(0.85, 'MSFT', 'Microsoft')
        score, src_sym, tgt_sym = decode_cursor(cursor)

        assert abs(score - 0.85) < 0.0001
        assert src_sym == 'MSFT'
        assert tgt_sym == 'Microsoft'

    def test_encode_decode_roundtrip(self):
        """Encoding then decoding should return original values."""
        original = (0.75, 'GOOG', 'Alphabet Inc')
        cursor = encode_cursor(*original)
        result = decode_cursor(cursor)

        assert abs(result[0] - original[0]) < 0.0001
        assert result[1] == original[1]
        assert result[2] == original[2]

    def test_decode_cursor_raises_on_invalid_base64(self):
        """Invalid base64 should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid cursor format"):
            decode_cursor("not-valid-base64!!!")

    def test_decode_cursor_raises_on_malformed_json(self):
        """Malformed JSON in cursor should raise ValueError."""
        import base64
        bad_cursor = base64.urlsafe_b64encode(b"not-json").decode()
        with pytest.raises(ValueError, match="Invalid cursor format"):
            decode_cursor(bad_cursor)

    def test_decode_cursor_raises_on_wrong_length(self):
        """JSON array with wrong length should raise ValueError."""
        import base64
        import json
        bad_cursor = base64.urlsafe_b64encode(json.dumps([1, 2]).encode()).decode()
        with pytest.raises((ValueError, IndexError)):
            decode_cursor(bad_cursor)

    def test_encode_cursor_handles_special_characters(self):
        """Cursor should handle special characters in strings."""
        cursor = encode_cursor(0.5, 'BRK.A', "Berkshire Hathaway Class A's")
        score, src, tgt = decode_cursor(cursor)

        assert src == 'BRK.A'
        assert tgt == "Berkshire Hathaway Class A's"

    def test_encode_cursor_handles_unicode(self):
        """Cursor should handle unicode characters."""
        cursor = encode_cursor(0.5, '日本株', '日本の会社')
        score, src, tgt = decode_cursor(cursor)

        assert src == '日本株'
        assert tgt == '日本の会社'


class TestFilterBuilder:
    """Tests for FilterBuilder query construction."""

    def test_empty_builder_returns_true(self):
        """Empty FilterBuilder should return 'TRUE' where clause."""
        builder = FilterBuilder()
        assert builder.where_clause == "TRUE"
        assert builder.params == []

    def test_add_exact_match(self):
        """Add exact match filter."""
        builder = FilterBuilder()
        builder.add('class_type', 'provider')

        assert 'class_type = $1' in builder.where_clause
        assert builder.params == ['provider']

    def test_add_partial_match(self):
        """Add partial match filter with LIKE."""
        builder = FilterBuilder()
        builder.add('name', 'Apple', partial_match=True)

        assert 'LOWER(name) LIKE LOWER($1)' in builder.where_clause
        assert builder.params == ['%Apple%']

    def test_add_list_filter(self):
        """Add IN clause from comma-separated list."""
        builder = FilterBuilder()
        builder.add('symbol', 'AAPL,MSFT,GOOG', is_list=True)

        assert 'symbol IN' in builder.where_clause
        assert '$1' in builder.where_clause
        assert '$2' in builder.where_clause
        assert '$3' in builder.where_clause
        assert builder.params == ['AAPL', 'MSFT', 'GOOG']

    def test_add_bool_filter(self):
        """Add boolean filter."""
        builder = FilterBuilder()
        builder.add('is_active', True)

        assert 'is_active = $1' in builder.where_clause
        assert builder.params == [True]

    def test_add_none_skipped(self):
        """None values should be skipped."""
        builder = FilterBuilder()
        builder.add('name', None)

        assert builder.where_clause == "TRUE"
        assert builder.params == []

    def test_add_empty_string_skipped(self):
        """Empty strings should be skipped."""
        builder = FilterBuilder()
        builder.add('name', '  ')

        assert builder.where_clause == "TRUE"
        assert builder.params == []

    def test_multiple_filters_combined_with_and(self):
        """Multiple filters should be combined with AND."""
        builder = FilterBuilder()
        builder.add('class_type', 'provider')
        builder.add('name', 'test', partial_match=True)

        assert 'AND' in builder.where_clause
        assert 'class_type = $1' in builder.where_clause
        assert 'LOWER(name) LIKE LOWER($2)' in builder.where_clause
        assert builder.params == ['provider', '%test%']

    def test_method_chaining(self):
        """Methods should return self for chaining."""
        builder = FilterBuilder()
        result = builder.add('type', 'a').add('name', 'b', partial_match=True)

        assert result is builder
        assert len(builder.params) == 2

    def test_custom_start_idx(self):
        """Builder should respect custom starting parameter index."""
        builder = FilterBuilder(start_idx=3)
        builder.add('name', 'test')

        assert '$3' in builder.where_clause
        assert builder.next_param_idx == 4

    def test_next_param_idx_tracking(self):
        """next_param_idx should track parameter indices correctly."""
        builder = FilterBuilder()
        assert builder.next_param_idx == 1

        builder.add('a', 'val1')
        assert builder.next_param_idx == 2

        builder.add('b', 'x,y,z', is_list=True)
        assert builder.next_param_idx == 5

    def test_url_decoding(self):
        """Values should be URL-decoded."""
        builder = FilterBuilder()
        builder.add('name', 'Apple%20Inc', partial_match=True)

        # %20 should be decoded to space
        assert builder.params == ['%Apple Inc%']

    def test_list_with_spaces(self):
        """List values should handle spaces around commas."""
        builder = FilterBuilder()
        builder.add('symbol', ' AAPL , MSFT , GOOG ', is_list=True)

        assert builder.params == ['AAPL', 'MSFT', 'GOOG']

    def test_empty_list_skipped(self):
        """Empty list after parsing should be skipped."""
        builder = FilterBuilder()
        builder.add('symbol', '  ,  ,  ', is_list=True)

        assert builder.where_clause == "TRUE"
        assert builder.params == []


class TestFilterBuilderIntegration:
    """Integration-style tests for FilterBuilder."""

    def test_realistic_asset_query(self):
        """Test a realistic asset filtering scenario."""
        builder = FilterBuilder()
        builder.add('class_name', 'EODHD', partial_match=True)
        builder.add('class_type', 'provider')
        builder.add('asset_class', 'equity')
        builder.add('symbol', 'AAP', partial_match=True)
        builder.add('is_active', None)  # Should be skipped

        assert builder.where_clause == (
            "LOWER(class_name) LIKE LOWER($1) AND "
            "class_type = $2 AND "
            "asset_class = $3 AND "
            "LOWER(symbol) LIKE LOWER($4)"
        )
        assert builder.params == ['%EODHD%', 'provider', 'equity', '%AAP%']
        assert builder.next_param_idx == 5

    def test_build_complete_query(self):
        """Test building a complete SQL query."""
        builder = FilterBuilder()
        builder.add('class_type', 'provider')
        builder.add('symbol', 'AAPL,MSFT', is_list=True)

        # Build a complete query
        query = f"""
            SELECT * FROM assets
            WHERE {builder.where_clause}
            ORDER BY symbol
            LIMIT ${builder.next_param_idx}
        """

        assert 'class_type = $1' in query
        assert 'symbol IN ($2, $3)' in query
        assert 'LIMIT $4' in query
