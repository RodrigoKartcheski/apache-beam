import carling
import pandas as pd
import apache_beam as beam

# Create a DataFrame containing the data that you want to index.
data = pd.DataFrame({
    "customer_id": [1, 2, 3, 4, 5],
    "product_id": [10, 20, 30, 40, 50],
    "quantity": [1, 2, 3, 4, 5]
})

# Create a carling.IndexBySingle object.
indexer = carling.IndexBySingle(data, "customer_id")

# Index the DataFrame using the carling.IndexBySingle object.
indexed_data = indexer.index(data)

# Print the indexed DataFrame.
print(indexed_data)
