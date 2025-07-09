def test_validate_connection(client):
    assert client.validate_connection()

def test_base_creation(client):
    assert client.create_base(
        base_name="Test",
        description="This is a Testbase",
        icon_color="#FF0000",
        prevent_duplicates=True
    )