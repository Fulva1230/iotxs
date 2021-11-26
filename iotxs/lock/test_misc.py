class Base:
    def __init__(self):
        self.x = 3
        print("base int get called")


class Derived(Base):
    pass


def test_base_init_get_called():
    derived = Derived()
    assert derived.x == 3
