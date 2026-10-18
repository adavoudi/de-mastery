def test_vacuous(submission):
    # Imports the submission but asserts nothing meaningful: the starter will PASS,
    # so gate G1 (starter must fail) is violated. This is on purpose.
    assert submission is not None
