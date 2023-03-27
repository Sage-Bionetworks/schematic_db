"""Stores the graph structure of the database schema"""

import networkx
from schematic_db.api_utils.api_utils import get_graph_by_edge_type


class SchemaGraph:
    """
    Stores the graph structure of the database schema
    """

    def __init__(self, schema_url: str) -> None:
        """
        Args:
            schema_url (str): The url of the schema in jsonld form.
        """
        self.schema_url = schema_url
        self.schema_graph = self.create_schema_graph()

    def create_schema_graph(self) -> networkx.DiGraph:
        """Retrieve the edges from schematic API and store in networkx.DiGraph()

        Returns:
            networkx.DiGraph: The edges of the graph
        """
        subgraph = get_graph_by_edge_type(self.schema_url, "requiresComponent")
        schema_graph = networkx.DiGraph()
        schema_graph.add_edges_from(subgraph)
        return schema_graph

    def create_sorted_object_name_list(self) -> list[str]:
        """
        Uses the schema graph to create a object name list such objects always come after ones they
         depend on.
        This order is how objects in a database should be built and/or updated.

        Returns:
            list[str]: A list of objects names
        """
        return list(reversed(list(networkx.topological_sort(self.schema_graph))))

    def get_neighbors(self, object_name: str) -> list[str]:
        """Gets the neighbors of the table in the schema graph

        Args:
            object_name (str): The name of the object to ge the neighbors of

        Returns:
            list[str]: A list of other objects
        """
        return list(self.schema_graph.neighbors(object_name))
