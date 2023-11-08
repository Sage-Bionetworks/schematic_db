"""Stores the graph structure of the database schema"""

import networkx
from schematic_db.api_utils.api_utils import get_graph_by_edge_type


class SchemaGraph:
    """
    Stores the graph structure of the database schema
    """

    def __init__(self, schema_url: str, use_schema_labels:bool = False) -> None:
        """
        Args:
            schema_url (str): The url of the schema in jsonld form.
            use_schema_labels (bool):
                What to use for column names from the schema
                If True attribute labels will be taken from the schema
                If False, attribute display names
        """
        self.schema_url = schema_url
        self.schema_graph = self.create_schema_graph(use_schema_labels)

    def create_schema_graph(self, use_schema_labels:bool=False) -> networkx.DiGraph:
        """Retrieve the edges from schematic API and store in networkx.DiGraph()

        Args:
            use_schema_labels (bool):
                What to use for column names from the schema
                If True attribute labels will be taken from the schema
                If False, attribute display names

        Returns:
            networkx.DiGraph: The edges of the graph
        """
        subgraph = get_graph_by_edge_type(
            schema_url=self.schema_url,
            relationship="requiresComponent",
            display_name_as_label=use_schema_labels
        )
        schema_graph = networkx.DiGraph()
        schema_graph.add_edges_from(subgraph)
        return schema_graph

    def create_sorted_table_name_list(self) -> list[str]:
        """
        Uses the schema graph to create a table name list where tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of table names
        """
        return list(reversed(list(networkx.topological_sort(self.schema_graph))))

    def get_neighbors(self, table_name: str) -> list[str]:
        """Gets the neighbors of the table in the schema graph

        Args:
            table_name (str): The name of the tables to get the neighbors of

        Returns:
            list[str]: A list of other tables
        """
        return list(self.schema_graph.neighbors(table_name))
