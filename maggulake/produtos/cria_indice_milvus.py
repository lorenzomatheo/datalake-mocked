from pymilvus import DataType, Function, FunctionType, MilvusClient


def get_schema(dimension: int = 1536):
    schema = MilvusClient.create_schema(
        auto_id=False,
        enable_dynamic_field=True,
    )

    schema.add_field(
        field_name="id", datatype=DataType.VARCHAR, max_length=36, is_primary=True
    )
    schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=dimension)
    schema.add_field(field_name="ean", datatype=DataType.VARCHAR, max_length=25)
    schema.add_field(
        field_name="lojas_com_produto", datatype=DataType.VARCHAR, max_length=65530
    )

    schema.add_field(
        field_name="ids_lojas_com_produto",
        datatype=DataType.ARRAY,
        element_type=DataType.VARCHAR,
        max_capacity=2000,
        max_length=36,
        nullable=True,
    )

    schema.add_field(
        field_name="principio_ativo",
        datatype=DataType.VARCHAR,
        max_length=1000,
        nullable=True,
    )
    schema.add_field(
        field_name="via_administracao",
        datatype=DataType.VARCHAR,
        max_length=1000,
        nullable=True,
    )
    schema.add_field(
        field_name="idade_recomendada",
        datatype=DataType.VARCHAR,
        max_length=100,
        nullable=True,
    )
    schema.add_field(
        field_name="sexo_recomendado",
        datatype=DataType.VARCHAR,
        max_length=100,
        nullable=True,
    )
    schema.add_field(field_name="eh_nao_tarjado", datatype=DataType.BOOL)
    schema.add_field(field_name="eh_nao_controlado", datatype=DataType.BOOL)

    schema.add_field(
        field_name="categorias",
        datatype=DataType.ARRAY,
        element_type=DataType.VARCHAR,
        max_capacity=50,
        max_length=200,
        nullable=True,
    )

    schema.add_field(
        field_name="informacoes_para_embeddings",
        datatype=DataType.VARCHAR,
        max_length=65530,
        enable_analyzer=True,
    )

    schema.add_field(field_name="sparse_bm25", datatype=DataType.SPARSE_FLOAT_VECTOR)
    bm25_function = Function(
        name="text_bm25_emb",
        input_field_names=["informacoes_para_embeddings"],
        output_field_names=["sparse_bm25"],
        function_type=FunctionType.BM25,
    )

    schema.add_function(bm25_function)

    return schema


def get_index_params(client):
    index_params = client.prepare_index_params()

    index_params.add_index(field_name="id")
    index_params.add_index(field_name="ean")

    # NOTE: esses índices são feitos para acelerar filtros booleanos e de igualdade
    # Muito importante para manter as recomendações rápidas
    index_params.add_index(field_name="lojas_com_produto")
    index_params.add_index(field_name="ids_lojas_com_produto")
    index_params.add_index(field_name="principio_ativo")
    index_params.add_index(field_name="eh_nao_tarjado")
    index_params.add_index(field_name="eh_nao_controlado")
    index_params.add_index(field_name="via_administracao")
    index_params.add_index(field_name="categorias")
    index_params.add_index(field_name="idade_recomendada")
    index_params.add_index(field_name="sexo_recomendado")

    index_params.add_index(
        field_name="vector",
        index_type="HNSW",
        metric_type="COSINE",
    )

    index_params.add_index(
        field_name="sparse_bm25", index_type="AUTOINDEX", metric_type="BM25"
    )

    return index_params


def create_collection(
    client: MilvusClient,
    collection_name: str,
    apaga_antiga=False,
    dimension: int = 1536,
):
    if apaga_antiga and client.has_collection(collection_name=collection_name):
        client.drop_collection(collection_name=collection_name)

    client.create_collection(
        collection_name=collection_name,
        schema=get_schema(dimension),
        index_params=get_index_params(client),
    )


def create_collection_chat(
    client: MilvusClient, collection_name: str, apaga_antiga=False
):
    if apaga_antiga and client.has_collection(collection_name=collection_name):
        client.drop_collection(collection_name=collection_name)

    schema = MilvusClient.create_schema(
        auto_id=False,
        enable_dynamic_field=True,
    )

    schema.add_field(
        field_name="id", datatype=DataType.VARCHAR, max_length=36, is_primary=True
    )
    schema.add_field(field_name="pergunta", datatype=DataType.VARCHAR, max_length=3000)
    schema.add_field(field_name="resposta", datatype=DataType.VARCHAR, max_length=3000)
    schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=3072)

    index_params = client.prepare_index_params()
    index_params.add_index(field_name="id")
    index_params.add_index(field_name="pergunta")
    index_params.add_index(field_name="resposta")
    index_params.add_index(
        field_name="vector",
        index_type="IVF_FLAT",
        metric_type="COSINE",
    )

    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        index_params=index_params,
    )
