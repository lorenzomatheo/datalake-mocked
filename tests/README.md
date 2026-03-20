# Testes do Projeto Datalake

Este diretório contém todos os testes do projeto, organizados em testes unitários e de integração.

## Estrutura de Diretórios

```
tests/
├── unit/                      # Testes unitários (testam uma coisa de cada vez)
│   ├── customerx/            # Testes do módulo CustomerX
│   │   ├── test_client.py    # Testes do client (com mocks)
│   │   ├── test_reset.py     # Testes de reset de sandbox
│   │   ├── test_contact_dto.py    # Testes de DTOs de contatos
│   │   ├── test_customer_dto.py   # Testes de DTOs de clientes
│   │   ├── test_conta_dto.py      # Testes de DTOs de contas/redes
│   │   ├── test_loja_dto.py       # Testes de DTOs de lojas
│   │   └── README.md         # Documentação específica do customerX
│   ├── maggulake/            # Testes de utilitários do maggulake
│   │   └── utils/
│   ├── test_categorias.py
│   ├── test_extended_enum.py
│   ├── test_iters.py
│   ├── test_numbers.py
│   ├── test_strings.py
│   ├── test_valida_ean.py
│   └── test_vector_search.py
└── integration/              # Testes de integração (testam sistema completo)
    └── customerx/            # Testes de integração do CustomerX (quando houver)

```

## Princípios de Organização

### Testes Unitários (`tests/unit/`)

- **Testam uma coisa de cada vez**: Cada teste valida um comportamento específico
- **Usam mocks**: Isolam dependências externas (APIs, bancos de dados, etc.)
- **São rápidos**: Executam em milissegundos
- **Não fazem I/O real**: Não fazem chamadas de rede, não acessam arquivos, etc.

### Testes de Integração (`tests/integration/`)

- **Testam o sistema como um todo**: Validam a integração entre componentes
- **Podem fazer I/O real**: Chamadas de API, acesso a banco de dados, etc.
- **São mais lentos**: Podem levar segundos ou minutos
- **Requerem setup especial**: Podem precisar de credenciais, ambientes específicos, etc.

## Executando os Testes

```bash
# Todos os testes
make test

# Apenas testes unitários
pytest tests/unit/

# Apenas testes de integração
pytest tests/integration/

# Testes de um módulo específico
pytest tests/unit/customerx/

# Com cobertura
make coverage

# Relatório HTML de cobertura
make coverage-html
```

## Adicionando Novos Testes

- **Determine o tipo de teste**:

  - Se testa uma função/classe isolada com mocks → `tests/unit/`
  - Se testa integração com sistemas externos → `tests/integration/`

- **Organize por módulo**:

  - Crie subdiretórios que reflitam a estrutura do código fonte
  - Ex: código em `maggulake/utils/integracoes/customerx/` → testes em `tests/unit/customerx/`

- **Nomeie descritivamente**:

  - Arquivos: `test_<nome_do_modulo>.py`
  - Classes: `Test<Classe>` ou `Test<Classe><Metodo>`
  - Métodos: `test_<comportamento_esperado>`

- **Use fixtures**:

  - Crie fixtures reutilizáveis para setup comum
  - Use `@pytest.fixture` para compartilhar código entre testes

## Padrões de Código

- Use **pytest** como framework de testes
- Use **type hints** nos testes
- Documente o que cada teste valida (docstrings)
- Use **mocks** (`unittest.mock`) para isolar dependências em testes unitários
- Organize testes em classes para agrupar testes relacionados

## Exemplos

### Teste Unitário

```python
from unittest.mock import Mock, patch
import pytest

class TestMinhaFuncao:
    @pytest.fixture
    def mock_client(self):
        return Mock()

    def test_funcao_com_parametros_validos(self, mock_client):
        """Testa que a função funciona com parâmetros válidos"""
        resultado = minha_funcao(mock_client, param="valor")
        assert resultado == "esperado"
```

### Teste de Integração

```python
class TestIntegracaoAPI:
    def test_busca_dados_reais(self):
        """Testa busca real na API (requer credenciais)"""
        client = ClientReal(api_key=os.getenv("API_KEY"))
        resultado = client.buscar_dados()
        assert len(resultado) > 0
```
