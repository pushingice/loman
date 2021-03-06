Change Log
==========

`0.2.1`_ (2017-12-29)
---------------------

* Added class-style definitions of computations

`0.2.0`_ (2017-12-05)
---------------------

* Added support for multithreading when calculating nodes
* Update to use networkx 2.0
* Added ``print_errors`` method
* Added ``force`` parameter to ``insert`` method to allow no recalculation if value is not updated
* BUGFIX: Fix behavior when calculation node overwritten with value node

`0.1.3`_ (2017-07-02)
---------------------

* Methods set_tag and clear_tag support lists or generators of tags. Method nodes_by_tag can retrieve a list of nodes with a specific tag.
* Remove set_tags and clear_tags.
* Add node computation timing data, accessible through tim attribute-style access or get_timing method.
* compute method can accept a list of nodes to compute.
* Loman now uses pydotplus for visualization. Internally, visualization has two steps: converting a Computation to a networkx visualization DAG, and then converting that to a pydotplus Dot object.
* Added view method - creates and opens a temporary pdf visualization.
* draw and view methods can show timing information with colors='timing' option

`0.1.2`_ (2017-04-28)
---------------------

* Add @node function decorator
* Add ConstantValue (with alias C) to provide constant values to function parameters without creating a placeholder node for that constant
* BUGFIX: Visualizing computations was broken in v0.1.1!

`0.1.1`_ (2017-04-25)
---------------------

* Support for Python 3.4 and 3.5
* Method and attribute-style accessors support lists of nodes
* Added support for node-tagging
* Compute method can optionally throw exceptions, for easier interactive debugging
* ``get_inputs`` method and ``i`` attribute-style access to get list of inputs to a node
* ``add_node`` takes optional inspect parameter to avoid inspection for performance
* ``add_node`` takes optional group to render graph layout with subgraphs
* ``draw_graphviz`` renamed to ``draw``
* ``draw_nx`` removed
* ``get_df`` renamed to ``to_df``
* ``get_value_dict`` renamed to ``to_dict``
* BUGFIX: implementation of _get_calc_nodes used by compute fixed
* BUGFIX: args parameters do not create spurious nodes
* BUGFIX: default function parameters do not cause placeholder node to be created
* BUGFIX: node states correctly updated when calling add_node with value parameter

`0.1.0`_ (2017-04-05)
---------------------

* Added documentation: Introduction, Quickstart and Strategies for Use
* Added docstrings to Computation methods
* Added logging
* Added ``v`` and ``s`` fields for attribute-style access to values and states of nodes
* BUGFIX: Detect cycles in ``compute_all``

`0.0.1`_ (2017-03-24)
---------------------

* Computation object with ``add_node``, ``insert``, ``compute``, ``compute_all``, ``state``, ``value``, ``set_stale`` methods
* Computation object can be drawn with ``draw_graphviz`` method
* Nodes can be updated in place
* Computation handles exceptions in node computation, storing exception and traceback
* Can specify mapping between function parameters and input nodes
* Convenience methods: ``add_named_tuple_expansion``, ``add_map_node``, ``get_df``, ``get_value_dict``, ``insert_from``, ``insert_multi``
* Convenience method
* Computation objects can be serialized
* Computation objects can be shallow-copied with ``copy``
* Unit tests
* Runs under Python 2.7, 3.6