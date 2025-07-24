## YouTube Scrapper DPTs

Este repositório contém um pipeline em Python para coletar metadados e transcrições de vídeos de canais do YouTube usando a YouTube Data API v3 e o pacote `youtube-transcript-api`, com orquestração via Poetry. Abaixo está um guia completo para instalar, configurar, executar e estender o pipeline.

---

## Sumário

* [Pré‑requisitos](#pré‑requisitos)
* [Instalação](#instalação)
* [Estrutura do Projeto](#estrutura-do-projeto)
* [Como Executar](#como-executar)
* [Opções de Configuração](#opções-de-configuração)
* [Customizando via `pyproject.toml`](#customizando-via-pyprojecttoml)
* [Pipeline – Explicação dos Stages](#pipeline–explicação-dos-stages)
* [Boas Práticas](#boas-práticas)
* [Contribuição](#contribuição)
* [Licença](#licença)

---

## Pré‑requisitos

1. **Git** ≥ 2.20 instalado
2. **Poetry** ≥ 1.5 instalado
3. **Python** 3.11 (conforme especificado em `pyproject.toml`)
4. **Credenciais**:

   * Chave da YouTube Data API v3 (defina `YOUTUBE_API_KEY` ou passe `--api-key`)
   * (Opcional) Variáveis de ambiente para proxy (Webshare ou genérico)

---

## Instalação

1. Clone o repositório:

   ```bash
   git clone git@github.com:Wesley-Costa/youtube-scrapper-dpts.git
   cd youtube-scrapper-dpts
   ```

2. Instale as dependências via Poetry:

   ```bash
   poetry install
   ```

3. Ative o ambiente virtual:

   ```bash
   poetry shell
   ```

---

## Estrutura do Projeto

```
youtube-scrapper-dpts/
├── .gitignore
├── README.md
├── pyproject.toml      # Configuração do Poetry e script
├── poetry.lock
├── src/
│   └── projeto_masterdegree/
│       ├── dados/      # CSVs de entrada e parciais
│       └── scripts/
│           ├── youtube_pipeline_v4.py  # Script principal
│           └── test.py                 # Teste de proxy/transcrições
└── tests/              # Skeleton para testes
```

* **`src/projeto_masterdegree/scripts/youtube_pipeline_v4.py`**: entry‑point do pipeline
* **`pyproject.toml`**: declara o script `youtube-pipeline`
* **`data_scrapper_v3/`** (padrão): diretório onde arquivos parciais e final são gerados

---

## Como Executar

### 1. Pipeline Completo

```bash
poetry run youtube-pipeline \
  --input path/para/seu/canais.csv \
  --api-key SUA_YOUTUBE_API_KEY \
  --data-dir ./meus_dados \
  --max-channels 100 \
  --max-videos 10 \
  [--reset]
```

* `--input`: CSV com colunas `id,nome,siglaPartido,youtube`
* `--api-key, -k`: YouTube Data API v3
* `--data-dir`: pasta de saída (logs, parciais e final)
* `--max-channels`: limita quantos canais processar
* `--max-videos`: limita quantos vídeos buscar por canal
* `--reset`: apaga parciais e estado antes de rodar

### 2. Teste de Transcrição (Proxy)

```bash
export WEBSHARE_USER="seu_user"
export WEBSHARE_PASS="sua_senha"
poetry run python src/projeto_masterdegree/scripts/test.py
```

Serve para validar se o proxy está funcionando e retorna os primeiros caracteres da transcrição.

---

## Opções de Configuração

Além dos parâmetros de linha de comando, o pipeline lê:

* **Variáveis de ambiente**

  * `YOUTUBE_API_KEY` – se não passar `--api-key`
  * `WEBSHARE_USER` e `WEBSHARE_PASS` – para proxy Webshare
  * `HTTP_PROXY` / `HTTPS_PROXY` – para proxy genérico

* **Funções no código**

  * `resolve_channel_id`: escolha de método para extrair Channel ID
  * `fetch_transcript_records`: backoff e retries em transcrições

Se não definir proxy, `YouTubeTranscriptApi()` roda sem ele.

---

## Customizando via `pyproject.toml`

O `pyproject.toml` já declara:

```toml
[tool.poetry.scripts]
youtube-pipeline = "projeto_masterdegree.scripts.youtube_pipeline_v4:main"
```

Para alterar o script usado ou suas configurações:

1. Abra `pyproject.toml`.
2. Em `[tool.poetry.scripts]`, ajuste o caminho para seu novo módulo ou função.
3. Se necessário, adicione dependências em `[tool.poetry.dependencies]` ou `[tool.poetry.group.dev.dependencies]`.
4. Rode `poetry install` para sincronizar.

Exemplo, mudando a versão do pipeline:

```toml
[tool.poetry.scripts]
yt-pipeline-v5 = "projeto_masterdegree.scripts.youtube_pipeline_v5:main"
```

E então executar:

```bash
poetry run yt-pipeline-v5 --input ... 
```

---

## Pipeline – Explicação dos Stages

1. **Stage 1 – canais**

   * Lê CSV de canais, resolve `channelId`, coleta lista de vídeos e grava em `base_partial.csv`.
2. **Stage 2 – meta**

   * Para cada vídeo, usa `youtube.videos().list` para capturar `snippet` e `statistics` e grava em `meta_partial.csv`.
3. **Stage 3 – transcrições**

   * Usa `YouTubeTranscriptApi` para baixar transcrições, aplica backoff e grava em `trans_partial.csv`.
4. **Stage 4 – consolidação**

   * Agrupa segmentos, gera coluna `full_transcript`, mergeia todas as tabelas e salva `youtube_data_full.csv`.

Cada stage grava seu progresso e atualiza `state.json` para permitir retomada em caso de falha.

---

## Boas Práticas

* **Use `.gitignore`** para não versionar CSVs parciais, logs e venv.
* **Mantenha a API Key em variável de ambiente**, nunca no repositório.
* **Controle de versão do pipeline**: crie branches para versões novas e ajuste o script via `pyproject.toml`.
* **Logs**: consulte `pipeline.log` para diagnóstico de erros.
* **Testes**: comece a preencher `tests/` usando `pytest` para cada função do pipeline.

---

## Contribuição

1. Fork este repositório.
2. Crie uma branch com sua feature/bugfix:

   ```bash
   git checkout -b feature/nome-da-feature
   ```
3. Faça commits atômicos e siga Conventional Commits.
4. Abra um Pull Request descrevendo a mudança.
5. Aguarde revisão e feedback.

---
