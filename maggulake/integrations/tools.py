import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

# Palavras comuns em português, gerado via LLM.
STOP_WORDS_PT = {
    "a",
    "o",
    "e",
    "é",
    "de",
    "do",
    "da",
    "dos",
    "das",
    "um",
    "uma",
    "em",
    "para",
    "com",
    "que",
    "por",
    "na",
    "no",
    "os",
    "as",
    "sobre",
    "sob",
    "seu",
    "sua",
    "seus",
    "suas",
    "ser",
    "ter",
    "foi",
    "era",
    "ao",
    "aos",
    "nas",
    "nos",
}


def _remove_stop_words(frases: list[str]) -> list[str]:
    # Preprocessing function to remove common words
    return [
        " ".join([word for word in phrase.lower().split() if word not in STOP_WORDS_PT])
        for phrase in frases
    ]


def calcula_frase_media_por_embeddings(frases: list[str]) -> str:
    """
    Retorna a frase que melhor representa um conjunto de frases, para isso
    calcula embeddings de cada frase e seleciona a frase mais próxima da média
    dos embeddings.
    """

    if len(frases) == 0:
        raise ValueError("Nenhuma frase para calcular a média")

    if len(frases) == 1:
        return frases[0]

    processed_frases = _remove_stop_words(frases)

    # Cria um "bag-of-words" model
    vetorizador = CountVectorizer().fit_transform(processed_frases)
    vectors = vetorizador.toarray()

    # Calcula a similaridade de cosseno entre as frases
    cosine_similarities = np.dot(vectors, vectors.T)
    scores = cosine_similarities.sum(axis=1) - np.diag(cosine_similarities)

    # Encontra e retorna a frase com maior similaridade
    best_phrase_index = scores.argmax()
    return frases[best_phrase_index]
