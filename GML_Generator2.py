import json
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
        # Process in chunks
        return pd.read_csv(self.csv_filepath, chunksize=10000)

    def add_node(self, address, alias):
        if address not in self.nodes:
            new_id = self.node_count
            self.nodes[address] = new_id
            self.node_count += 1
            if address in self.known_addresses:
                label = f"{self.known_addresses[address]['type']} ({alias})"
            else:
                label = f"{address} ({alias})"
            return f'  node [\n  id {new_id}\n  label "{label}"\n]\n'
        return ''

    def generate_gml(self):
        total_rows_processed = 0
        for df_chunk in self.fetch_data():
            gml_nodes = ''
            edge_Info = {}

            for _, row in df_chunk.iterrows():
                gml_nodes += self.add_node(row['from_address'], row['alias_from'])
                gml_nodes += self.add_node(row['to_address'], row['alias_to'])

                edge_key = (row['from_address'], row['to_address'])
                edge_data = edge_Info.setdefault(edge_key, {'weight': 0, 'tx_direction': set(), 'transaction_type': set(), 'method_abi': set(), 'timestamps': [], 'gas_fees': [], 'gas_prices_paid': [], 'correlation_ids': []})

                edge_data['method_abi'].add(row['method_abi'])
                edge_data['transaction_type'].add(row['transaction_type'])
                edge_data['tx_direction'].add(row['tx_direction'])
                edge_data['weight'] += 1
                edge_data['timestamps'].append(pd.to_datetime(row['timestamp']))
                edge_data['gas_fees'].append(float(row['gas_fee']))
                edge_data['gas_prices_paid'].append(float(row['gas_price_paid']))
                edge_data['correlation_ids'].append(row['correlationid'])

            self.gml_data += gml_nodes

            for (from_address, to_address), data in edge_Info.items():
                from_id = self.nodes[from_address]
                to_id = self.nodes[to_address]
                most_common_method = max(data['method_abi'], key=data['method_abi'].count)  
                transaction_type = ', '.join(data['transaction_type'])
                tx_direction = ', '.join(data['tx_direction'])
                weight = data['weight']
                avg_gas_price = np.mean(data['gas_prices_paid'])
                avg_gas_fee = np.mean(data['gas_fees'])
                time_range = f"{min(data['timestamps']).isoformat()} to {max(data['timestamps']).isoformat()}"

                self.gml_data += (
                    f"  edge [\n"
                    f"    source {from_id}\n"
                    f"    target {to_id}\n"
                    f"    label \"{most_common_method}\"\n"
                    f"    transactionType \"{transaction_type}\"\n"
                    f"    xDirection \"{tx_direction}\"\n"
                    f"    all_methods \"{', '.join(data['method_abi'])}\"\n"
                    f"    weight {weight}\n"
                    f"    avg_timestamps \"{time_range}\"\n"
                    f"    avg_gas_fee \"{avg_gas_fee:.6f} ETH\"\n"
                    f"    avg_gas_paid \"{avg_gas_price:.6f} ETH\"\n"
                    f"    correlationids \"{', '.join(data['correlation_ids'])}\"\n"
                    f"  ]\n"
                )
                
            total_rows_processed += len(df_chunk)
            print(f"Processed {total_rows_processed} rows...")

        self.gml_data += "]"

        with open('output.gml', 'w') as file:
            file.write(self.gml_data)

        print("GML file generated successfully.")

if __name__ == "__main__":
    csv_filename = "transaction_data.csv" 
    json_filename = "tokenContracts.json"
    gml_generator = GMLGenerator(csv_filename)
    gml_generator.generate_gml()
