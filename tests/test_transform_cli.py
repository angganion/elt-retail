def test_run_step_rejects_unknown_layer():
    from transform.transform import run_step

    try:
        run_step("platinum")
    except ValueError as error:
        assert "platinum" in str(error)
    else:
        raise AssertionError("unknown transform layer was accepted")
