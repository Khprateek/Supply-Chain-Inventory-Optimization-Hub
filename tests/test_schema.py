from google.cloud import bigquery
client = bigquery.Client()
table = client.get_table('smart-supply-and-inventory.raw_supply_chain.raw_bridge_product_supplier')
print([f.name for f in table.schema])
