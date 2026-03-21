# Caso Monark — Hipótese 1 (rede temporal)

Repositório mínimo para o MVP do caso **Monark**: análise de hipótese 1 sobre evolução da rede em janelas temporais (JSONL → métricas → GEXF).

## Conteúdo

| Pasta / arquivo | Descrição |
|-----------------|-----------|
| `notebooks/02_monark_hipotese1.ipynb` | Notebook principal (rode a partir da raiz deste repositório para `Path.cwd()` apontar para a raiz). |
| `data/raw/` | Fonte classificada `tweets_classificados_monark.jsonl` (reprodutibilidade / pré-processamento). |
| `data/processed/monark/` | Janelas 3h/6h e diárias em JSONL usadas pelo notebook. |
| `data/outputs/monark/` | Saídas do pipeline (GEXF, tabelas) compatíveis com o notebook. |
| `reports/h1_monark/` | Saídas geradas pelo próprio notebook (métricas em janela, GEXF da hipótese 1, etc.). |
| `src/` | Scripts chamados pelo notebook (`windowed_metrics.py`, `jsonl_to_gexf.py`, opcionalmente `ego_isolation_timeseries.py`) e pré-processamento `split_monark_jsonl_by_sp_time.py`. |

## Organização temporal do caso Monark

A estrutura temporal do caso Monark foi reorganizada da seguinte forma:

- **PRE**: dias 07/02/2022 e 08/02/2022 agregados em um único arquivo:
  - `monark_pre_2022-02-07_00-00_2022-02-08_23-59.*`
- **Clímax em alta resolução**: recortes de 3h e 6h entre a noite de 08/02 e o dia 09/02.
- **Pós-clímax**: arquivos diários de 10/02/2022 a 14/02/2022.
- **Arquivos diários de 07/02, 08/02 e 09/02**: movidos para `data/archive/monark_daily_legacy/`.

Essa organização foi adotada para refletir melhor a densidade real do evento e evitar redundância entre recortes diários e intradiários.

## Ambiente

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

## Execução

1. Abrir o Jupyter a partir da **raiz** deste repositório (ou usar Colab: `cd` para a raiz após o clone conforme a primeira célula do notebook).
2. Executar `notebooks/02_monark_hipotese1.ipynb`.

Para regenerar os JSONL a partir do JSONL bruto classificado:

```bash
python src/split_monark_jsonl_by_sp_time.py
```

(Iso grava em `data/processed/monark/`; confira se deseja sobrescrever arquivos já versionados.)

## Tamanho dos dados

Os artefatos JSONL/GEXF neste repositório somam da ordem de **~15 MB** no total; são adequados para GitHub sem LFS. Se no futuro houver coletas brutas muito maiores, use [Git LFS](https://git-lfs.github.com/) ou hospede os dumps fora do repositório e documente apenas URLs.

## O que está fora de escopo

Este repositório **não** inclui outros notebooks da pasta de trabalho original (Eduardo Bueno, Karol Conká, Wagner, etc.), nem o notebook genérico `02_caso_generico_hipotese1.ipynb`, nem pipelines de NLP/BERT. O material do caso Monark que permaneceu no projeto original está duplicado aqui de forma organizada; o projeto original não foi apagado.
