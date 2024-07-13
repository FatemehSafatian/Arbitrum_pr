
import pandas as pd
from sqlalchemy import create_engine
import networkx as nx
import matplotlib.pyplot as plt
import os
from Generate_unique_node import create_address_alias_pairs,AddressAliasPair

class GMLGenerator:
    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
        self.G = nx.DiGraph()
        self.nodes = {}
        self.node_count = 0
        self.gml_data = "graph [\n directed 1\n"

    def fetch_data(self):
        df = pd.read_csv(self.csv_filepath)
        return df

    def add_node(self, address , alias):
        newPair=AddressAliasPair(address, alias, self.node_count)
        if address not in self.nodes:
            self.node_count += 1
            self.nodes[address] = newPair
            return  f'  node [\n  id {newPair.id}\n  label \"{address}\"\n  alias \"{alias}\"\n]\n'
          
        return ''
            
    def generate_gml(self):
        df = self.fetch_data()
        
        gml_nodes =''
        for _, row in df.iterrows():
            gml_nodes +=self.add_node(row['from_address'],  row['alias_from'])
        
        for _, row in df.iterrows():
            gml_nodes +=self.add_node(row['to_address'], row['alias_to'])

        self.gml_data += gml_nodes

        edge_Info = {}
        for _, row in df.iterrows():
            
            from_address= row['from_address']
            to_address= row['to_address']
            method_abi=row['method_abi']
            tx_direction=row['tx_direction']
            transaction_type=row['transaction_type']
            timestamp=row['timestamp']
            gas_fee=row['gas_fee']
            gas_price_paid=row['gas_price_paid']
            correlationid=row['correlationid']

            edge_key = (from_address, to_address)

            if edge_key not in edge_Info:
                edge_Info[edge_key] = {'weight': 0, 'tx_direction': set(), 'transaction_type': set(), 'method_abi': set(), 'timestamp':[], 'gas_fee':[], 'gas_prices_paid':[], 
                                       'correlationId':[]}
            
            if tx_direction not in edge_Info[edge_key]['tx_direction']:
                edge_Info[edge_key]['tx_direction'].add(tx_direction)

            if transaction_type not in edge_Info[edge_key]['transaction_type']:
                edge_Info[edge_key]['transaction_type'].add(transaction_type)

            if method_abi not in edge_Info[edge_key]['method_abi']:
                edge_Info[edge_key]['method_abi'].add(method_abi)
                edge_Info[edge_key]['weight'] = 1
            else:
                edge_Info[edge_key]['weight']+= 1

            edge_Info[edge_key]['timestamp'].append(timestamp)
            edge_Info[edge_key]['gas_fee'].append(gas_fee)
            edge_Info[edge_key]['gas_prices_paid'].append(gas_price_paid)
            edge_Info[edge_key]['correlationId'].append(correlationid)
                
        for (from_address, to_address), data in edge_Info.items():
            from_info =self.nodes[from_address ]  
            to_info = self.nodes[to_address]
            label = ', '.join(data['method_abi'])  
            transaction_type=', '.join(data['transaction_type'])
            tx_direction=', '.join(data['tx_direction'])
            weight = data['weight']
            timestamp = ', '.join(map(str, data['timestamp']))
            gas_fee = ', '.join(map(str, data['gas_fee']))
            gas_prices_paid = ', '.join(map(str, data['gas_prices_paid']))
            correlationid = ', '.join(data['correlationid'])

            # self.add_edge(from_info.id, to_info.id, label, transaction_type, tx_direction, timestamp, gas_fee, gas_prices_paid, correlationid, value=1)  
            self.gml_data += f"  edge [\n    source {from_info.id}\n    target {to_info.id}\n    label \"{label}\"\n    transactionType \"{transaction_type}\"\n    xDirection \"{tx_direction}\"\n   weight {weight}\n    timestamps \"{timestamp}\"\n    gas_fees \"{gas_fee}\"\n    gas_prices_paid \"{gas_prices_paid}\"\n    correlationids \"{correlationid}\"\n ]\n"


        self.gml_data += "]"

        with open('output.gml', 'w') as file:
            file.write(self.gml_data)

        print("GML file generated successfully.")

def load_and_visualize_gml(path):
    try:
        G = nx.read_gml(path)
        plt.figure(figsize=(12, 12))
        pos = nx.spring_layout(G) 
        # Draw the graph using the 'alias' attribute for the label information
        label_dict = {node[0]: node[1]['alias'] for node in G.nodes(data=True)}  # Extracting alias for each node
        nx.draw(G, pos, labels=label_dict, with_labels=True, node_color='skyblue', edge_color='#FF5733', node_size=500, font_size=8, font_color='black')
        plt.title('Graph Visualization with Alias')
        plt.show()
    except Exception as e:
        print("An error occurred while reading GML:", e)

def main():
    
    csv_filename = "transaction_data.csv" 
    if not os.path.exists("output.gml"):
        gml_generator = GMLGenerator(csv_filename)
        gml_generator.generate_gml()
    else:
       load_and_visualize_gml("output.gml")

if __name__ == "__main__":
    main()
