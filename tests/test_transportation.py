from process_waste.transportation import identify


def test_identify(assets_path):
    log_path = assets_path / 'PurchasingExampleModified.csv'
    result = identify(log_path, parallel_run=False)
    assert result is not None
    assert result['handoff'] is not None
    assert result['pingpong'] is not None
