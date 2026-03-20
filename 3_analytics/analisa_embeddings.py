# Databricks notebook source

# MAGIC %pip install tensorflow tensorboard==2.17.1 umap-learn adjustText

# COMMAND ----------
dbutils.library.restartPython()


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# COMMAND ----------
import umap
from adjustText import adjust_text
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.metrics.pairwise import cosine_similarity

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------
stage = "prod"
# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)

# COMMAND ----------
delta_table_path = "production.refined.produtos_com_embeddings"

# COMMAND ----------
produtos_foco = postgres.get_eans_em_campanha(spark)

# COMMAND ----------
# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("analisa_embedings").config(conf=config).getOrCreate()
)

# Leitura de tabela
df = spark.read.table(delta_table_path)
colunas = [
    "ean",
    "nome",
    "marca",
    "descricao",
    "embedding",
    "eans_alternativos",
]
# Select embeddings and metadata columns
embeddings_df = df.select(*colunas).filter(F.col('nome').isNotNull())

# COMMAND ----------
# Convert Spark DataFrame to Pandas
pandas_df = embeddings_df.toPandas()

# COMMAND ----------
# Convert embeddings to NumPy array
embeddings = np.stack(pandas_df["embedding"].values)

# Save metadata (e.g., product names, categories)
metadata = pandas_df[['marca', 'ean', 'eans_alternativos']]

# COMMAND ----------
# Reduce dimensionality
umap_reducer = umap.UMAP(n_components=3)
reduced_embeddings = umap_reducer.fit_transform(embeddings)
print(f"Shape after UMAP: {reduced_embeddings.shape}")


# COMMAND ----------
def plot_most_similar_products(
    metadata: pd.DataFrame,
    reduced_embeddings: np.ndarray,
    chosen_ean: str,
    top_n: int = 10,
) -> None:
    """
    Plots the chosen product (filtered by EAN) and its most similar products based on cosine similarity.
    Ensures that alternative EANs from the 'eans_alternativos' column are filtered out before computing similarity.

    Parameters:
    - metadata (DataFrame): PySpark DataFrame containing product metadata, including 'ean' and 'eans_alternativos'.
    - reduced_embeddings (np.ndarray): 2D array of shape (n_samples, 2) with reduced embeddings.
    - chosen_ean (str): EAN code of the chosen product.
    - top_n (int): Number of most similar products to display (including the chosen product).

    Returns:
    - None: Displays a scatter plot of the embeddings.
    """

    # Ensure metadata has an index aligned with reduced_embeddings
    metadata = metadata.copy().reset_index(drop=True)

    if chosen_ean not in metadata["ean"].values:
        raise ValueError(f"EAN {chosen_ean} not found in metadata.")

    # Find the index of the chosen product
    chosen_idx = metadata.index[metadata["ean"] == chosen_ean][0]

    # Extract alternative EANs
    alternative_eans = metadata.loc[chosen_idx, "eans_alternativos"]
    if not isinstance(alternative_eans, list):
        alternative_eans = []

    alternative_eans = set(alternative_eans)

    # Remove rows where 'ean' is in alternative_eans (but keep the chosen product)
    filtered_metadata = metadata[~metadata["ean"].isin(alternative_eans)].copy()

    if chosen_ean not in filtered_metadata["ean"].values:
        filtered_metadata = pd.concat([filtered_metadata, metadata.loc[[chosen_idx]]])

    # Reset index after filtering
    filtered_metadata = filtered_metadata.reset_index(drop=True)

    # Align reduced_embeddings with the new filtered metadata
    filtered_indices = filtered_metadata.index.values
    filtered_embeddings = reduced_embeddings[filtered_indices]

    # Find the new chosen index after filtering
    new_chosen_idx = filtered_metadata.index[filtered_metadata["ean"] == chosen_ean][0]

    # Add embedding coordinates to metadata
    filtered_metadata["x"] = filtered_embeddings[:, 0]
    filtered_metadata["y"] = filtered_embeddings[:, 1]

    # Compute similarity with all other products
    chosen_embedding = filtered_embeddings[new_chosen_idx].reshape(1, -1)
    similarities = cosine_similarity(chosen_embedding, filtered_embeddings)[0]

    # Get top N most similar products
    most_similar_indices = np.argsort(similarities)[::-1][:top_n]

    # Mark chosen and similar products in metadata
    filtered_metadata["similar"] = "Others"
    filtered_metadata.loc[most_similar_indices, "similar"] = "Similar Product"
    filtered_metadata.loc[new_chosen_idx, "similar"] = "Chosen Product"

    # Filter metadata for visualization
    plot_metadata = filtered_metadata[
        filtered_metadata["similar"].isin(["Similar Product", "Chosen Product"])
    ]

    # Set seaborn style
    sns.set_style("darkgrid", {"grid.color": ".6", "grid.linestyle": ":"})

    # Plot the scatter plot
    plt.figure(figsize=(12, 10))
    sns.scatterplot(
        x="x",
        y="y",
        hue="similar",
        style="similar",
        palette={
            "Others": "lightgray",
            "Similar Product": "blue",
            "Chosen Product": "red",
        },
        data=plot_metadata,
        alpha=0.6,
        edgecolor="black",
    )

    # Annotate product names with automatic text adjustment
    texts = []
    for i in most_similar_indices:
        name = filtered_metadata.marca[i]
        x, y = filtered_metadata.x[i], filtered_metadata.y[i]

        if i == new_chosen_idx:
            color, fontweight = "red", "bold"
        else:
            color, fontweight = "black", "normal"

        texts.append(
            plt.text(x, y, name, fontsize=10, color=color, fontweight=fontweight)
        )

    # Automatically adjust text positions to prevent overlap
    adjust_text(texts, arrowprops={'arrowstyle': '-', 'color': 'gray', 'lw': 0.5})

    # Set plot title and legend
    plt.title("Analisando Embeddings")
    plt.legend()
    plt.show()


# COMMAND ----------
# teste
plot_most_similar_products(metadata, reduced_embeddings, '7896006204657', top_n=10)


# COMMAND ----------
for produto_campanha in produtos_foco:
    print(f"EAN produto em campanha: {produto_campanha}")
    plot_most_similar_products(metadata, reduced_embeddings, produto_campanha, top_n=10)
