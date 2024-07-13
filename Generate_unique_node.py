import json
import pandas as pd

class AddressAliasPair:
  def __init__(self, address, alias, id):
        self.address = address
        self.alias = alias
        self.id = id

def create_address_alias_pairs(df, address, alias, starting_id):

    df[address] = df['json_data'].apply(lambda x: x[address])
    df[alias] = df['json_data'].apply(lambda x: x[alias])
    df['id'] = starting_id
    
    unique_addresses = df.drop_duplicates(subset=[address]).copy()
    
    unique_addresses['id'] = range(starting_id, starting_id + len(unique_addresses))

    unique_pairs = unique_addresses.apply(lambda row: AddressAliasPair(row[address], row[alias], row['id']), axis=1)

    return unique_pairs.tolist()

