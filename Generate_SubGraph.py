import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

class GenerateSubGraph:
    # def __init__(self):
        

    def generate_subgraph(self, gml):

        subgraph_edges = []

        for u, v, data in gml.edges(data=True):
            if 'Internal' in data.get('direction', ''):
                subgraph_edges.append((u, v))

        subgraph = gml.edge_subgraph(subgraph_edges).copy()

        nx.write_gml(subgraph, 'subgraph_internal.gml')
    
    def extract_first_timestamp(self, timestamp_range):
        return timestamp_range.split(' to ')[0]
   
    def generate_time(self, gml):
        edge_data = []
        for u, v, data in gml.edges(data=True):
            if 'timestamps' in data:
                gas_fees_cleaned = data['gasFees'].replace(',', '')
                edge_data.append({
                                'source': u,
                                'target': v,
                                'timestamp':self. extract_first_timestamp(data['timestamps']),
                                'gas_fee': float(gas_fees_cleaned.split()[0]),
                                'transaction_type': data['transactionType']
                                })

        df = pd.DataFrame(edge_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        hourly_transactions = df.resample('H').size()
        hourly_gas_fees = df.resample('H')['gas_fee'].sum()

        plt.figure(figsize=(12, 6))
        plt.plot(hourly_transactions, label='Hourly Transactions')
        plt.plot(hourly_gas_fees, label='Hourly Gas Fees')
        plt.legend()
        plt.show()

        fig, ax1 = plt.subplots(figsize=(12, 6))

        color = 'tab:blue'
        ax1.set_xlabel('Timestamp')
        ax1.set_ylabel('Hourly Transactions', color=color)
        ax1.plot(hourly_transactions, label='Hourly Transactions', color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
        color = 'tab:orange'
        ax2.set_ylabel('Hourly Gas Fees', color=color)  # we already handled the x-label with ax1
        ax2.plot(hourly_gas_fees, label='Hourly Gas Fees', color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()  # otherwise the right y-label is slightly clipped
        plt.title('Hourly Transactions and Gas Fees')
        plt.show()

        print("Total transactions:", df.shape[0])
        print("Total gas fees (ETH):", df['gas_fee'].sum())

        peak_hour = hourly_transactions.idxmax()
        print(f"Peak transaction hour: {peak_hour} with {hourly_transactions.max()} transactions")

        correlation = hourly_transactions.corr(hourly_gas_fees)
        print("Correlation between transactions and gas fees:", correlation)

    def generate_otherInfo(self, gml):

        edges_data = []
        for u, v, data in gml.edges(data=True):
            edge_info = {
                "source": u,
                "target": v,
                "label": data.get("label", "unknown"),
                "transactionType": data.get("transactionType", "unknown"),
                "direction": data.get("direction", "unknown"),
                "weight": data.get("weight", 1),
                "timestamps": data.get("timestamps", ""),
                "gasFees": data.get("gasFees", "0 ETH"),
                "gasPrices": data.get("gasPrices", "0 ETH"),
                "correlationIds": data.get("correlationIds", ""),
                "avgTimestamps": data.get("avgTimestamps", ""),
                "avgGasFee": data.get("avgGasFee", "0 ETH"),
                "avgGasPaid": data.get("avgGasPaid", "0 ETH")
            }

        edges_data.append(edge_info)

        edges_df = pd.DataFrame(edges_data)

        # Convert relevant columns to appropriate data types
        edges_df['timestamps'] = pd.to_datetime(edges_df['timestamps'].str.split(" to ").str[0], errors='coerce')
        edges_df['avgTimestamps'] = pd.to_datetime(edges_df['avgTimestamps'].str.split(" to ").str[0], errors='coerce')
        edges_df['gasFees'] = edges_df['gasFees'].str.replace(" ETH", "").astype(float)
        edges_df['avgGasFee'] = edges_df['avgGasFee'].str.replace(" ETH", "").astype(float)
        edges_df['weight'] = edges_df['weight'].astype(int)

        # Step 3: Perform Analysis
        # Temporal Analysis
        # plt.figure(figsize=(10, 5))
        # plt.plot(edges_df['timestamps'], edges_df['gasFees'], marker='o', linestyle='-', color='b')
        # plt.xlabel('Timestamp')
        # plt.ylabel('Gas Fees (ETH)')
        # plt.title('Gas Fees Over Time')
        # plt.grid(True)
        # plt.show()

        # # Economic Analysis
        # plt.figure(figsize=(10, 5))
        # plt.bar(edges_df['timestamps'].dt.strftime('%Y-%m-%d'), edges_df['gasFees'], color='g')
        # plt.xlabel('Timestamp')
        # plt.ylabel('Gas Fees (ETH)')
        # plt.title('Gas Fees by Date')
        # plt.xticks(rotation=45)
        # plt.grid(True)
        # plt.show()

        # Weight Analysis
        plt.figure(figsize=(10, 5))
        plt.hist(edges_df['weight'], bins=30, color='purple', edgecolor='black')
        plt.xlabel('Weight')
        plt.ylabel('Frequency')
        plt.title('Transaction Weight Distribution')
        plt.grid(True)
        plt.show()

        # Correlation ID Analysis
        unique_correlation_ids = edges_df['correlationIds'].nunique()
        print(f"Number of unique correlation IDs: {unique_correlation_ids}")

        # Example: Plotting transactions with the same correlation ID
        sample_correlation_id = edges_df['correlationIds'].iloc[0]
        sample_df = edges_df[edges_df['correlationIds'] == sample_correlation_id]

        plt.figure(figsize=(10, 5))
        plt.plot(sample_df['timestamps'], sample_df['gasFees'], marker='o', linestyle='-', color='r')
        plt.xlabel('Timestamp')
        plt.ylabel('Gas Fees (ETH)')
        plt.title(f'Gas Fees Over Time for Correlation ID: {sample_correlation_id}')
        plt.grid(True)
        plt.show()

        edges_df.head()
   
def main():
    G1 = nx.read_gml('output_weakly_L200.gml')
    G = nx.DiGraph(G1)
    
    # GenerateSubGraph().generate_subgraph(G)
    GenerateSubGraph().generate_time(G)

if __name__ == "__main__":
    main()