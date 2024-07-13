from collections import Counter
from datetime import datetime
import json
import time
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import os
from Generate_unique_node import create_address_alias_pairs, AddressAliasPair

class GMLGenerator:
    def __init__(self, csv_filepath, json_filepath):
        self.csv_filepath = csv_filepath
        self.json_filepath = json_filepath
        self.G = nx.DiGraph()
        self.nodes = {}
        self.node_count = 0
        self.gml_data = "graph [\n directed 1\n"
        self.load_json_data()
    
    def load_json_data(self):
        with open(self.json_filepath, 'r') as file:
            self.known_addresses = json.load(file)

    def fetch_data(self):
        return pd.read_csv(self.csv_filepath, chunksize=10000)


    def add_node(self, address, alias):
        if address not in self.nodes:
            new_id = self.node_count
            self.nodes[address] = new_id
            self.node_count += 1
            if address in self.known_addresses:
                label = f"{self.known_addresses[address]['type']}"

            else:
                label = f"{address}"
            return f'  node [\n  id {new_id}\n  label "{label}"\n    alias \"{alias}\"\n]\n'
        return ''

    def generate_gml(self):
        total_rows_processed = 0
        edge_Info = {}
        gml_entries = ["graph [\n directed 1\n"]
        gml_nodes_set = set()
        unique_edges_set = set()

        for df_chunk in self.fetch_data():
            for _, row in df_chunk.iterrows():

                edge_key = (row['from_address'], row['to_address'])

                if edge_key not in unique_edges_set:
                    unique_edges_set.add(edge_key)
                    if row['from_address'] not in gml_nodes_set:
                        gml_entries.append(self.add_node(row['from_address'], row['alias_from']))
                        gml_nodes_set.add(row['from_address'])
                    
                    if row['to_address'] not in gml_nodes_set:
                        gml_entries.append(self.add_node(row['to_address'], row['alias_to']))
                        gml_nodes_set.add(row['to_address'])

                    edge_key = (row['from_address'], row['to_address'])

                
                    edge_Info.setdefault(edge_key, {
                    'weight': 0, 'tx_direction': set(), 'transaction_type': set(), 
                    'method_abi': set(), 'timestamp': [], 'gas_fee': [], 
                    'gas_prices_paid': [], 'correlationid': []
                        })
                
                
                edge_Info[edge_key]['method_abi'].add(row['method_abi'])
                edge_Info[edge_key]['weight']+= 1            
                edge_Info[edge_key]['transaction_type'].add(row['transaction_type'])
                edge_Info[edge_key]['tx_direction'].add(row['tx_direction'])
                edge_Info[edge_key]['timestamp'].append(pd.to_datetime(row['timestamp']))
                edge_Info[edge_key]['gas_fee'].append(float(row['gas_fee']))
                edge_Info[edge_key]['gas_prices_paid'].append(float(row['gas_price_paid']))
                edge_Info[edge_key]['correlationid'].append(row['correlationid'])
      
            total_rows_processed += len(df_chunk)
            print(f"Processed {total_rows_processed} rows...")
        
        counter = 0  # Initialize a counter to track the number of iterations
        total_edges = len(edge_Info)  # Total number of edges to process
        start_time = time.time()

        for (from_address, to_address), data in edge_Info.items():
                from_id = self.nodes[from_address]
                to_id = self.nodes[to_address]
                abi_method = ', '.join(data['method_abi'])   
                transaction_type = ', '.join(data['transaction_type'])
                direction = ', '.join(data['tx_direction'])
                gas_prices = ', '.join(format(gp, ".6f") for gp in data['gas_prices_paid'])
                avg_gas_price = np.mean(data['gas_prices_paid'])
                gas_fees = ', '.join(format(gf, ".6f") for gf in data['gas_fee'])
                avg_gas_fee = np.mean(data['gas_fee'])
                time_range = f"{min(data['timestamp']).isoformat()} to {max(data['timestamp']).isoformat()}"
                correlation_ids = ', '.join(data['correlationid'])

                edge_entry = (
                f"  edge [\n"
                f"    source {from_id}\n"
                f"    target {to_id}\n"
                f"    label \"{abi_method}\"\n"
                f"    transactionType \"{transaction_type}\"\n"
                f"    direction \"{direction}\"\n"
                f"    weight {data['weight']}\n"
                f"    timestamps \"{time_range}\"\n"
                f"    gasFees \"{gas_fees} ETH\"\n"
                f"    gasPrices \"{gas_prices} ETH\"\n"
                f"    correlationIds \"{correlation_ids}\"\n"
                f"    avgTimestamps \"{time_range}\"\n"
                f"    avgGasFee \"{avg_gas_fee:.6f} ETH\"\n"
                f"    avgGasPaid \"{avg_gas_price:.6f} ETH\"\n"
                f"  ]\n"
                )
                gml_entries.append(edge_entry)

                # counter += 1 
                # if counter % 10000 == 0:
                #     current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                #     elapsed_time = time.time() - start_time  # Calculate elapsed time
                #     print(f"[{current_time}] Processed {counter}/{total_edges} edges in {elapsed_time:.2f} seconds")

                

        gml_entries.append("]")

        final_gml_data = ''.join(gml_entries)
        with open('output_third.gml', 'w') as file:
            file.write(final_gml_data)

        print("GML file generated successfully.")

    def load_and_visualize_gml(self, path):
        try:
            G = nx.read_gml(path)
            plt.figure(figsize=(12, 12))
            pos = nx.spring_layout(G)
            label_dict = {node[0]: node[1]['alias'] for node in G.nodes(data=True)}
            nx.draw(G, pos, labels=label_dict, with_labels=True, node_color='skyblue', edge_color='#FF5733', node_size=500, font_size=8, font_color='black')
            plt.title('Graph Visualization with Alias')
            plt.show()
        except Exception as e:
            print("An error occurred while reading GML:", e)
    
    def Get_WeaklyComponent(self, gml_path, output_path):
        # Load the directed graph
        G = nx.read_gml(gml_path)

        largest_component = max(nx.weakly_connected_components(G), key=len)

        H = G.subgraph(largest_component).copy()

        nx.write_gml(H, output_path)
        print(f"Largest weakly connected component saved to {output_path}")

    def Get_betweennes(self, gml_path):
        G = nx.read_gml(gml_path)

        betweenness = nx.betweenness_centrality(G)
    
        top_10_betweenness = sorted(betweenness.items(), key=lambda item: item[1], reverse=True)[:10]
    
        print("Top 10 Betweenness Centrality:")
        for node, value in top_10_betweenness:
            print(f"Node: {node}, Betweenness Centrality: {value}")



if __name__ == "__main__":
    csv_filename = "transaction_data_third.csv" 
    json_filename = "tokenContracts.json"
    import numpy as np

    gml_generator = GMLGenerator(csv_filename, json_filename)
    gml_generator.generate_gml()
    # gml_generator.load_and_visualize_gml('outputL.gml')
    input_gml = 'output_third.gml'
    output_gml = 'output_third_weakly.gml'

    gml_generator.Get_WeaklyComponent(input_gml, output_gml)
    # gml_generator.Get_betweennes('output_weakly.gml')
