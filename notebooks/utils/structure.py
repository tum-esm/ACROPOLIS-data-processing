import abc
import logging


class Component(abc.ABC):
    """Basic building block of the pipeline.

    Each component defines a series of operations inside an execute method.
    """

    @abc.abstractmethod
    def execute(self):
        pass


class Composite(Component):
    """Allows combining multiple components into what looks like a single component.

    The allows for a tree structure, where each node is a composite and each leaf is a
    component. This makes the pipeline very flexible and allows for arbitrarily
    complex structures.
    """

    def __init__(self):
        self._components: list[Component] = []

    def add(self, *args) -> None:
        """Add one or multiple components to the composite."""
        for component in args:
            self._components.append(component)

    def execute(self, *args):
        """Execute each component in the order they were added.

        The output/s of one component is/are passed as input/s to the next one.
        """
        for component in self._components:
            if not isinstance(component, Composite):
                logging.info(f"Executing '{type(component).__name__}'")
            if not isinstance(args, tuple):
                args = tuple() if args is None else (args,)
            args = component.execute(*args)
        return args
