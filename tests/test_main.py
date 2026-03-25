import pytest

from montblanc_pipeline.main import _CATALOG_RE


@pytest.mark.parametrize("name", [
    "montblanc_dev",
    "montblanc_prod",
    "montblanc_test",
    "catalog123",
    "my-catalog",
    "MY_CATALOG",
])
def test_valid_catalog_names(name: str):
    """Valid catalog names match the regex."""
    assert _CATALOG_RE.match(name)


@pytest.mark.parametrize("name", [
    "my catalog",       # space
    "my.catalog",       # dot
    "my;catalog",       # semicolon
    "`my_catalog`",     # backticks
    "my/catalog",       # slash
    "",                 # empty
])
def test_invalid_catalog_names(name: str):
    """Invalid catalog names do not match the regex."""
    assert not _CATALOG_RE.match(name)
