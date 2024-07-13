import networkx as nx

class GenerateSubGraph:
    def __init__(self, gml):
        self.gml = gml

    def generate_subgraph(self, direction, output_filename):
        subgraph_nodes = set()

        for u, v, data in self.gml.edges(data=True):
            if data.get('direction') == direction:
                subgraph_nodes.add(u)
                subgraph_nodes.add(v)

        subgraph = self.gml.subgraph(subgraph_nodes)
        nx.write_gml(subgraph, output_filename)

def main():
    G1 = nx.read_gml('output_weakly_L200.gml')
    G = nx.DiGraph(G1)
    
    generator = GenerateSubGraph(G)
    directions = [
        ("Inbound (External to Arbitrum)", "subgraph_inbound.gml"),
        ("Outbound (Arbitrum to External)", "subgraph_outbound.gml"),
        ("Internal", "subgraph_internal.gml")
    ]
    
    for direction, filename in directions:
        generator.generate_subgraph(direction, filename)

if __name__ == "__main__":
    main()