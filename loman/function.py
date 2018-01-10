from abc import ABCMeta, abstractproperty
import inspect

from loman import compat


def get_function_specifier(func):
    if callable(func):
        return LocalFunctionSpecifier(func)
    elif isinstance(func, FunctionSpecifier):
        return func
    else:
        raise Exception("Unexpected function specifier: {}".format(func))


class FunctionSpecifier:
    __metaclass__ = ABCMeta

    @abstractproperty
    def func(self):
        pass

    @property
    def signature(self):
        return compat.get_signature(self.func)

    @property
    def source(self):
        return inspect.getsource(self.func)

    @property
    def name(self):
        return self.func.__name__


class LocalFunctionSpecifier(FunctionSpecifier):
    def __init__(self, func):
        self._func = func

    @property
    def func(self):
        return self._func
