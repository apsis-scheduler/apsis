from apsis.lib import memo

# -------------------------------------------------------------------------------


def test_property():
    class C:
        count = 0

        def __init__(self, x):
            self.__x = x

        @memo.property
        def x(self):
            C.count += 1
            return self.__x

        @memo.property
        def y(self):
            C.count += 1
            return 17

    c0 = C(42)
    assert C.count == 0
    assert c0.x == 42
    assert C.count == 1
    assert c0.x == 42
    assert C.count == 1
    assert c0.y == 17
    assert C.count == 2
    assert c0.y == 17
    assert C.count == 2
    assert c0.x == 42
    assert C.count == 2

    c1 = C(43)
    assert C.count == 2
    assert c1.x == 43
    assert C.count == 3
    assert c1.x == 43
    assert C.count == 3
    assert c1.y == 17
    assert C.count == 4
    assert c1.y == 17
    assert C.count == 4
    assert c1.x == 43
    assert C.count == 4

    assert c0.y == 17
    assert C.count == 4
    assert c0.x == 42
    assert C.count == 4
