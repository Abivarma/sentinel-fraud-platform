"""
Integration tests: Feast feature store configuration and entity/view consistency.

These tests do NOT require a running Feast server or Redis. They validate
the static configuration YAML and Python definitions for correctness.

Run with: pytest -m integration tests/integration/test_feast_materialization.py
"""

import sys
from pathlib import Path
import pytest

pytestmark = pytest.mark.integration

# Absolute path to the feature repo
FEATURE_REPO_DIR = Path(__file__).parent.parent.parent / "feature_store" / "feature_repo"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_feature_store_yaml_valid():
    """
    Load feature_store/feature_repo/feature_store.yaml and assert:
      - project == "sentinel"
      - offline_store.type == "spark"
      - online_store.type == "redis"
    """
    import yaml

    yaml_path = FEATURE_REPO_DIR / "feature_store.yaml"
    assert yaml_path.exists(), f"feature_store.yaml not found at {yaml_path}"

    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    assert config.get("project") == "sentinel", (
        f"Expected project == 'sentinel', got {config.get('project')!r}"
    )

    offline = config.get("offline_store", {})
    assert offline.get("type") == "spark", (
        f"Expected offline_store.type == 'spark', got {offline.get('type')!r}"
    )

    online = config.get("online_store", {})
    assert online.get("type") == "redis", (
        f"Expected online_store.type == 'redis', got {online.get('type')!r}"
    )


@pytest.mark.integration
def test_feature_view_entity_consistency():
    """
    Parse the entity and feature view definitions (without importing Feast)
    by reading the Python source as text and asserting that:
      - The three valid entity names appear in entities.py
      - Each feature view file references at least one valid entity name
    """
    valid_entity_names = {"customer_id", "merchant_id", "transaction_id"}

    # --- Verify entities.py defines all three ---
    entities_path = FEATURE_REPO_DIR / "entities.py"
    assert entities_path.exists(), f"entities.py not found at {entities_path}"

    entities_src = entities_path.read_text()
    for name in valid_entity_names:
        assert f'name="{name}"' in entities_src, (
            f"Entity '{name}' not found in entities.py"
        )

    # --- Verify feature_views.py references valid entity names ---
    views_path = FEATURE_REPO_DIR / "feature_views.py"
    assert views_path.exists(), f"feature_views.py not found at {views_path}"

    views_src = views_path.read_text()

    # Each of the three entity variables should appear in the views file
    entity_var_names = {"customer", "merchant", "transaction"}
    for var in entity_var_names:
        assert var in views_src, (
            f"Entity variable '{var}' not referenced in feature_views.py"
        )

    # The three known FeatureView names should all be defined
    expected_views = {"customer_feature_view", "merchant_feature_view", "transaction_feature_view"}
    for view_name in expected_views:
        assert view_name in views_src, (
            f"FeatureView '{view_name}' not found in feature_views.py"
        )


@pytest.mark.integration
def test_feature_view_ttl_configured():
    """
    Verify that all three feature views have a TTL configured in feature_views.py
    (timedelta should appear once per view definition).
    """
    views_path = FEATURE_REPO_DIR / "feature_views.py"
    views_src = views_path.read_text()

    # timedelta appears in the imports and once per FeatureView with ttl=
    import re
    ttl_matches = re.findall(r"ttl\s*=\s*timedelta\s*\(", views_src)

    # We have three FeatureViews; each must declare a TTL
    assert len(ttl_matches) == 3, (
        f"Expected 3 TTL declarations in feature_views.py, found {len(ttl_matches)}"
    )


@pytest.mark.integration
def test_data_sources_defined():
    """
    Verify that data_sources.py defines the three source objects referenced
    by the feature views (customer_features_source, merchant_features_source,
    transaction_features_source).
    """
    sources_path = FEATURE_REPO_DIR / "data_sources.py"
    assert sources_path.exists(), f"data_sources.py not found at {sources_path}"

    sources_src = sources_path.read_text()

    expected_sources = [
        "customer_features_source",
        "merchant_features_source",
        "transaction_features_source",
    ]
    for src_name in expected_sources:
        assert src_name in sources_src, (
            f"Data source '{src_name}' not defined in data_sources.py"
        )
