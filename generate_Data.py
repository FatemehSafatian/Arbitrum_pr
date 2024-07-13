import pandas as pd
from sqlalchemy import create_engine
import os

class DataExporter:
    def __init__(self, engine):
        self.engine = engine

    def fetch_data(self):
        query = """
        SELECT 
            s.block_number AS bknum,
            s.tx_hash, s.from_address, s.to_address,
            s.transaction_type, s.tx_direction,
            s.timestamp, s.gas_fee, s.gas_price_paid, s.correlationid,
            a.method_id,
            a.method_signature as method_abi,
            CONCAT(SUBSTRING(s.from_address FROM 3 FOR 4), '...', SUBSTRING(s.from_address FROM LENGTH(s.from_address) - 3), ' (', s.id, '_F)') AS alias_from,
            CONCAT(SUBSTRING(s.to_address FROM 3 FOR 4), '...', SUBSTRING(s.to_address FROM LENGTH(s.to_address) - 3), ' (', s.id, '_T)') AS alias_to
        FROM 
            L2_transactions AS s
        INNER JOIN 
            abis AS a ON s.method_abi_id = a.id
            where  a.method_signature<> 'startBlock'  AND s.from_address <> s.to_address
        LIMIT 200923 OFFSET 0;
        
        """
        df = pd.read_sql(query, self.engine)
        return df

    def export_data_to_csv(self, filename):
        df = self.fetch_data()
        df.to_csv(filename, index=False)
        print(f"Data exported successfully to {filename}")

def main():
    engine = create_engine('postgresql://postgres:1234@localhost:5432/ArbitrumDB')
    data_exporter = DataExporter(engine)
    output_filename = "transaction_data_third.csv"
    data_exporter.export_data_to_csv(output_filename)

if __name__ == "__main__":
    main()