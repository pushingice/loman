from loman import Computation, States, MapException
import six
from collections import namedtuple
import random


def test_basic():
    def b(a):
        return a + 1

    def c(a):
        return 2 * a

    def d(b, c):
        return b + c

    comp = Computation()
    comp.add_node("a")
    comp.add_node("b", b)
    comp.add_node("c", c)
    comp.add_node("d", d)

    assert comp.state('a') == States.UNINITIALIZED
    assert comp.state('c') == States.UNINITIALIZED
    assert comp.state('b') == States.UNINITIALIZED
    assert comp.state('d') == States.UNINITIALIZED

    comp.insert("a", 1)
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.COMPUTABLE
    assert comp.state('c') == States.COMPUTABLE
    assert comp.state('d') == States.STALE
    assert comp.value('a') == 1

    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.state('d') == States.UPTODATE
    assert comp.value('a') == 1
    assert comp.value('b') == 2
    assert comp.value('c') == 2
    assert comp.value('d') == 4

    comp.insert("a", 2)
    comp.compute("b")
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.COMPUTABLE
    assert comp.state('d') == States.STALE
    assert comp.value('a') == 2
    assert comp.value('b') == 3

    assert set(comp._get_calc_nodes("d")) == set(['c', 'd'])


def test_parameter_mapping():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda x: x + 1, kwds={'x': 'a'})
    comp.insert('a', 1)
    comp.compute_all()
    assert comp.state('b') == States.UPTODATE
    assert comp.value('b') == 2


def test_parameter_mapping_2():
    def b(x):
        return x + 1

    def c(x):
        return 2 * x

    def d(x, y):
        return x + y

    comp = Computation()
    comp.add_node("a")
    comp.add_node("b", b, kwds={'x': 'a'})
    comp.add_node("c", c, kwds={'x': 'a'})
    comp.add_node("d", d, kwds={'x': 'b', 'y': 'c'})

    comp.insert("a", 1)
    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.state('d') == States.UPTODATE
    assert comp.value('a') == 1
    assert comp.value('b') == 2
    assert comp.value('c') == 2
    assert comp.value('d') == 4



def test_serialization():
    def b(x):
        return x + 1

    def c(x):
        return 2 * x

    def d(x, y):
        return x + y

    comp = Computation()
    comp.add_node("a")
    comp.add_node("b", b, kwds={'x': 'a'})
    comp.add_node("c", c, kwds={'x': 'a'})
    comp.add_node("d", d, kwds={'x': 'b', 'y': 'c'})

    comp.insert("a", 1)
    comp.compute_all()
    f = six.BytesIO()
    comp.write_dill(f)

    f.seek(0)
    foo = Computation.read_dill(f)

    assert set(comp.dag.nodes()) == set(foo.dag.nodes())
    for n in comp.dag.nodes():
        assert comp.dag.node[n].get('state', None) == foo.dag.node[n].get('state', None)
        assert comp.dag.node[n].get('value', None) == foo.dag.node[n].get('value', None)


def test_namedtuple_expansion():
    comp = Computation()
    Coordinate = namedtuple("Coordinate", ['x', 'y'])
    comp.add_node("a")
    comp.add_named_tuple_expansion("a", Coordinate)
    comp.insert("a", Coordinate(1, 2))
    comp.compute_all()
    assert comp.value("a.x") == 1
    assert comp.value("a.y") == 2


def test_zero_parameter_functions():
    comp = Computation()

    def a():
        return 1
    comp.add_node('a', a)
    assert comp.state('a') == States.COMPUTABLE

    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.value('a') == 1


def test_change_structure():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a + 1)
    comp.add_node('c', lambda a: 2 * a)
    comp.add_node('d', lambda c: 10 * c)
    comp.insert('a', 10)
    comp.compute_all()
    assert comp['d'] == (States.UPTODATE, 200)

    comp.add_node('d', lambda b: 5 * b)
    assert comp.state('d') == States.COMPUTABLE

    comp.compute_all()
    assert comp['d'] == (States.UPTODATE, 55)


def test_exceptions():
    def b(a):
        raise Exception("Infinite sadness")
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', b)
    comp.insert('a', 1)
    comp.compute_all()

    assert comp.state('b') == States.ERROR
    assert str(comp.value('b').exception) == "Infinite sadness"


def test_exceptions_2():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a/0)
    comp.add_node('c', lambda b: b+1)

    comp.insert('a', 1)
    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.ERROR
    assert comp.state('c') == States.STALE
    assert comp.value('a') == 1

    comp.add_node('b', lambda a: a+1)
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.COMPUTABLE
    assert comp.state('c') == States.STALE
    assert comp.value('a') == 1

    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.value('a') == 1
    assert comp.value('b') == 2
    assert comp.value('c') == 3


def test_exception_compute_all():
    comp = Computation()
    comp.add_node('a', value=1)
    comp.add_node('b', lambda a: a/0)
    comp.add_node('c', lambda b: b)
    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.state('b') == States.ERROR
    assert comp.state('c') == States.STALE


def test_exception_compute():
    comp = Computation()
    comp.add_node('a', value=1)
    comp.add_node('b', lambda a: a/0)
    comp.add_node('c', lambda b: b)
    comp.compute('c')
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.state('b') == States.ERROR
    assert comp.state('c') == States.STALE


def test_update_function():
    def b1(a):
        return a + 1
    def b2(a):
        return a + 2
    def c(b):
        return 10 * b
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', b1)
    comp.add_node('c', c)

    comp.insert('a', 1)

    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.value('a') == 1
    assert comp.value('b') == 2
    assert comp.value('c') == 20

    comp.add_node('b', b2)
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.COMPUTABLE
    assert comp.state('c') == States.STALE
    assert comp.value('a') == 1

    comp.compute_all()
    assert comp.state('a') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.value('a') == 1
    assert comp.value('b') == 3
    assert comp.value('c') == 30


def test_update_function_with_structure_change():
    def b1(a1):
        return a1 + 1
    def b2(a2):
        return a2 + 2
    def c(b):
        return 10 * b
    comp = Computation()
    comp.add_node('a1')
    comp.add_node('a2')
    comp.add_node('b', b1)
    comp.add_node('c', c)

    comp.insert('a1', 1)
    comp.insert('a2', 2)

    comp.compute_all()
    assert comp.state('a1') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.value('a1') == 1
    assert comp.value('b') == 2
    assert comp.value('c') == 20

    comp.add_node('b', b2)
    assert comp.state('a2') == States.UPTODATE
    assert comp.state('b') == States.COMPUTABLE
    assert comp.state('c') == States.STALE
    assert comp.value('a2') == 2

    comp.compute_all()
    assert comp.state('a2') == States.UPTODATE
    assert comp.state('b') == States.UPTODATE
    assert comp.state('c') == States.UPTODATE
    assert comp.value('a2') == 2
    assert comp.value('b') == 4
    assert comp.value('c') == 40


def test_copy():
    comp = Computation()
    comp.add_node("a")
    comp.add_node("b", lambda a: a + 1)

    comp.insert("a", 1)
    comp.compute_all()
    assert comp.state("a") == States.UPTODATE
    assert comp.state("b") == States.UPTODATE
    assert comp.value("a") == 1
    assert comp.value("b") == 2

    comp2 = comp.copy()
    comp2.insert("a", 5)
    comp2.compute_all()
    assert comp2.state("a") == States.UPTODATE
    assert comp2.state("b") == States.UPTODATE
    assert comp2.value("a") == 5
    assert comp2.value("b") == 6

    assert comp.state("a") == States.UPTODATE
    assert comp.state("b") == States.UPTODATE
    assert comp.value("a") == 1
    assert comp.value("b") == 2


def test_copy_2():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a + 1)
    comp.insert('a', 1)

    comp2 = comp.copy()
    assert comp2['a'] == (States.UPTODATE, 1)
    assert comp2.state('b') == States.COMPUTABLE

    comp2.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.state('b') == States.COMPUTABLE
    assert comp2['a'] == (States.UPTODATE, 1)
    assert comp2['b'] == (States.UPTODATE, 2)



def test_serialization_skip_flag():
    comp = Computation()
    comp.add_node("a")
    comp.add_node("b", lambda a: a + 1, serialize=False)
    comp.add_node("c", lambda b: b + 1)

    comp.insert("a", 1)
    comp.compute_all()
    f = six.BytesIO()
    comp.write_dill(f)

    assert comp.state("a") == States.UPTODATE
    assert comp.state("b") == States.UPTODATE
    assert comp.state("c") == States.UPTODATE
    assert comp.value("a") == 1
    assert comp.value("b") == 2
    assert comp.value("c") == 3

    f.seek(0)
    comp2 = Computation.read_dill(f)
    assert comp2.state("a") == States.UPTODATE
    assert comp2.state("b") == States.UNINITIALIZED
    assert comp2.state("c") == States.UPTODATE
    assert comp2.value("a") == 1
    assert comp2.value("c") == 3


def test_insert_many():
    comp = Computation()
    l = list(range(100))
    random.shuffle(l)
    prev = None
    for x in l:
        if prev is None:
            comp.add_node(x)
        else:
            comp.add_node(x, lambda n: n+1, kwds={'n': prev})
        prev = x
    comp.insert_many([(x, x) for x in range(100)])
    for x in range(100):
        assert comp[x] == (States.UPTODATE, x)


def test_insert_from():
    comp = Computation()
    comp.add_node("a")
    comp.add_node("b", lambda a: a + 1, serialize=False)
    comp.add_node("c", lambda b: b + 1)

    comp.insert("a", 1)
    comp2 = comp.copy()

    comp.compute_all()
    assert comp.state("a") == States.UPTODATE
    assert comp.state("b") == States.UPTODATE
    assert comp.state("c") == States.UPTODATE
    assert comp.value("a") == 1
    assert comp.value("b") == 2
    assert comp.value("c") == 3
    assert comp2.state("a") == States.UPTODATE
    assert comp2.state("b") == States.COMPUTABLE
    assert comp2.state("c") == States.STALE
    assert comp2.value("a") == 1

    comp2.insert_from(comp, ['a', 'c'])
    assert comp.state("a") == States.UPTODATE
    assert comp.state("b") == States.UPTODATE
    assert comp.state("c") == States.UPTODATE
    assert comp.value("a") == 1
    assert comp.value("b") == 2
    assert comp.value("c") == 3
    assert comp2.state("a") == States.UPTODATE
    assert comp2.state("b") == States.COMPUTABLE
    assert comp2.state("c") == States.UPTODATE
    assert comp2.value("a") == 1
    assert comp2.value("c") == 3


def test_insert_from_large():
    def make_chain(comp, f, l):
        prev = None
        for i in l:
            if prev is None:
                comp.add_node(i)
            else:
                comp.add_node(i, f, kwds={"x": prev})
            prev = i

    def add_one(x):
        return x + 1

    comp1 = Computation()
    make_chain(comp1, add_one, range(100))
    comp1.insert(0, 0)
    comp1.compute_all()

    for i in range(100):
        assert comp1.state(i) == States.UPTODATE
        assert comp1.value(i) == i

    comp2 = Computation()
    l1 = list(range(100))
    random.shuffle(l1)
    make_chain(comp2, add_one, l1)

    comp2.insert_from(comp1)
    for i in range(100):
        assert comp2.state(i) == States.UPTODATE
        assert comp2.value(i) == i


def test_get_df():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a + 1)
    comp.insert('a', 1)
    comp.compute_all()
    df = comp.get_df()

    assert df.loc['a', 'value'] == 1
    assert df.loc['a', 'state'] == States.UPTODATE
    assert df.loc['b', 'value'] == 2
    assert df.loc['b', 'state'] == States.UPTODATE


def test_tuple_node_key():
    def add(a, b):
        return a + b

    comp = Computation()
    comp.add_node(('fib', 1))
    comp.add_node(('fib', 2))
    for i in range(3, 11):
        comp.add_node(('fib', i), add, kwds={'a': ('fib', i - 2), 'b': ('fib', i - 1)})

    comp.insert(('fib', 1), 0)
    comp.insert(('fib', 2), 1)
    comp.compute_all()

    assert comp.value(('fib', 10)) == 34


def test_get_item():
    comp = Computation()
    comp.add_node('a', lambda: 1)
    comp.add_node('b', lambda a: a + 1)
    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp['b'] == (States.UPTODATE, 2)


def test_set_stale():
    comp = Computation()
    comp.add_node('a', lambda: 1)
    comp.add_node('b', lambda a: a + 1)
    comp.compute_all()

    assert comp['a'] == (States.UPTODATE, 1)
    assert comp['b'] == (States.UPTODATE, 2)

    comp.set_stale('a')
    assert comp.state('a') == States.COMPUTABLE
    assert comp.state('b') == States.STALE

    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp['b'] == (States.UPTODATE, 2)


def test_error_stops_compute_all():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a/0)
    comp.add_node('c', lambda b: b+1)
    comp.insert('a', 1)
    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.state('b') == States.ERROR
    assert comp.state('c') == States.STALE


def test_error_stops_compute():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a/0)
    comp.add_node('c', lambda b: b+1)
    comp.insert('a', 1)
    comp.compute('c')
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.state('b') == States.ERROR
    assert comp.state('c') == States.STALE


def test_map_graph():
    subcomp = Computation()
    subcomp.add_node('a')
    subcomp.add_node('b', lambda a: 2*a)
    comp = Computation()
    comp.add_node('inputs')
    comp.add_map_node('results', 'inputs', subcomp, 'a', 'b')
    comp.insert('inputs', [1, 2, 3])
    comp.compute_all()
    assert comp['results'] == (States.UPTODATE, [2, 4, 6])


def test_map_graph_error():
    subcomp = Computation()
    subcomp.add_node('a')
    subcomp.add_node('b', lambda a: 1/(a-2))
    comp=Computation()
    comp.add_node('inputs')
    comp.add_map_node('results', 'inputs', subcomp, 'a', 'b')
    comp.insert('inputs', [1, 2, 3])
    comp.compute_all()
    assert comp.state('results') == States.ERROR
    assert isinstance(comp.value('results').exception, MapException)
    results = comp.value('results').exception.results
    assert results[0] == -1
    assert results[2] == 1
    assert isinstance(results[1], Computation)
    failed_graph = results[1]
    assert failed_graph.state('b') == States.ERROR

def test_placeholder():
    comp = Computation()
    comp.add_node('b', lambda a: a + 1)
    assert comp.state('a') == States.PLACEHOLDER
    assert comp.state('b') == States.UNINITIALIZED
    comp.add_node('a')
    assert comp.state('a') == States.UNINITIALIZED
    assert comp.state('b') == States.UNINITIALIZED
    comp.insert('a', 1)
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.state('b') == States.COMPUTABLE
    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp['b'] == (States.UPTODATE, 2)


def test_delete_predecessor():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a + 1)
    comp.insert('a', 1)
    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp['b'] == (States.UPTODATE, 2)
    comp.delete_node('a')
    assert comp.state('a') == States.PLACEHOLDER
    assert comp['b'] == (States.UPTODATE, 2)
    comp.delete_node('b')
    assert comp.dag.nodes() == []


def test_delete_successor():
    comp = Computation()
    comp.add_node('a')
    comp.add_node('b', lambda a: a + 1)
    comp.insert('a', 1)
    comp.compute_all()
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp['b'] == (States.UPTODATE, 2)
    comp.delete_node('b')
    assert comp['a'] == (States.UPTODATE, 1)
    assert comp.dag.nodes() == ['a']
    comp.delete_node('a')
    assert comp.dag.nodes() == []


def test_no_serialize_flag():
    comp = Computation()
    comp.add_node('a', serialize=False)
    comp.add_node('b', lambda a: a + 1)
    comp.insert('a', 1)
    comp.compute_all()

    f = six.BytesIO()
    comp.write_dill(f)
    f.seek(0)
    comp2 = Computation.read_dill(f)
    assert comp2.state('a') == States.UNINITIALIZED
    assert comp2['b'] == (States.UPTODATE, 2)


def test_value():
    comp = Computation()
    comp.add_node('a', value=10)
    comp.add_node('b', lambda a: a + 1)
    comp.add_node('c', lambda a: 2 * a)
    comp.add_node('d', lambda c: 10 * c)
    comp.compute_all()
    assert comp['d'] == (States.UPTODATE, 200)


def test_args():
    def f(*args):
        return sum(args)
    comp = Computation()
    comp.add_node('a', value=1)
    comp.add_node('b', value=1)
    comp.add_node('c', value=1)
    comp.add_node('d', f, args=['a', 'b', 'c'])
    comp.compute_all()
    assert comp['d'] == (States.UPTODATE, 3)


def test_kwds():
    def f(**kwds):
        return set(kwds.keys()), sum(kwds.values())
    comp = Computation()
    comp.add_node('a', value=1)
    comp.add_node('b', value=1)
    comp.add_node('c', value=1)
    comp.add_node('d', f, kwds={'a': 'a', 'b': 'b', 'c': 'c'})
    assert comp.state('d') == States.COMPUTABLE
    comp.compute_all()
    assert comp['d'] == (States.UPTODATE, (set(['a', 'b', 'c']), 3))


def test_args_and_kwds():
    def f(a, b, c, *args, **kwds):
        return locals()
    comp = Computation()
    comp.add_node('a', value='a')
    comp.add_node('b', value='b')
    comp.add_node('c', value='c')
    comp.add_node('p', value='p')
    comp.add_node('q', value='q')
    comp.add_node('r', value='r')
    comp.add_node('x', value='x')
    comp.add_node('y', value='y')
    comp.add_node('z', value='z')
    comp.add_node('res', func=f, args=['a', 'b', 'c', 'p', 'q', 'r'], kwds={'x': 'x', 'y': 'y', 'z': 'z'})
    comp.compute_all()
    assert comp.value('res') == {'a': 'a', 'b': 'b', 'c': 'c',
                                 'args': ('p', 'q', 'r'),
                                 'kwds': {'x': 'x', 'y': 'y', 'z': 'z'}}