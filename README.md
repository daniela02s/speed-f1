# Speed F1: Feature Store e Predição de Campeão Mundial

## Visão Geral do Projeto

Este projeto é a implementação completa de um **pipeline de Machine Learning (MLOps)**, focado em dados históricos e em tempo real da Fórmula 1 (F1). O objetivo principal é desenvolver um modelo que atribua a **probabilidade** de cada piloto se tornar o **Campeão Mundial de F1** ao final de cada corrida.

O pipeline utiliza a **Arquitetura Medallion** (Bronze, Silver, Gold) para o gerenciamento de dados e garante a reprodutibilidade usando o **Docker**.

***

## Créditos e Origem

Este projeto foi desenvolvido e documentado ao vivo, em uma série de lives e vídeos no canal **Teo Me Why** do YouTube.

> **Agradecimento Especial:** Agradecemos ao **Teo Me Why** por todo o conteúdo e orientação no desenvolvimento deste pipeline de dados.
>
> * **Série Completa no YouTube:** [Gravação da Série Completa no YouTube](https://www.youtube.com/playlist?list=PLvlkVRRKOYFRha5ExLDyf7jbOVII55JRH)
> * **Repositório Base:** [Repositório completo do projeto](https://github.com/TeoMeWhy/speed-f1)

***

## Arquitetura e Tecnologias

Todo o ambiente de desenvolvimento e produção é isolado e orquestrado via Docker Compose.

| Categoria | Tecnologia | Função |
| :--- | :--- | :--- |
| **Orquestração** | `docker compose` | Gerencia, constrói e inicia todos os serviços (ETL, App, MLflow). |
| **Processamento** | **Apache Spark** (via PySpark) | Processamento de dados em escala, consolidação e engenharia de *features*. |
| **Armazenamento** | **Delta Lake** | Formato de armazenamento para Atomicidade, Consistência, Isolamento e Durabilidade (ACID) na Camada de Dados. |
| **MLOps/Tracking**| **MLflow** | Rastreamento de experimentos, registro de modelos e gerenciamento da *Feature Store*. |
| **Visualização** | **Streamlit** (via `app.py`) | Criação do painel web interativo para exibir as previsões de probabilidade. |
| **Ambiente** | **WSL 2** | Subsystema Windows para Linux, usado para hospedar o ambiente Docker nativo. |

***

## Estrutura do Pipeline de Dados

O projeto segue a Arquitetura Medallion para garantir a qualidade e o gerenciamento de dados:

1.  **🟣 RAW Layer (`run_raw`):**
    * Coleta dados brutos de corridas (GPs e Sprints) de uma fonte externa.
    * Os dados são armazenados na pasta `./data/raw`.

2.  **🟤 BRONZE Layer (`run_bronze`):**
    * Os dados são lidos do RAW, têm o esquema aplicado e são consolidados em tabelas Spark/Delta Lake para persistência e consultas iniciais.

3.  **⚪ SILVER Layer (`run_silver`):**
    * **Feature Engineering:** Cria *features* complexas e séries temporais (`fs_drivers`) para cada piloto, após cada rodada.
    * **Tabelas Analíticas:** Gera tabelas prontas para consumo, como `champions` (histórico de títulos).

4.  **🟡 GOLD Layer (`run_gold`):**
    * **Predição:** O modelo mais recente (carregado do MLflow Registry) é aplicado ao Feature Store (`fs_drivers`).
    * **Resultado Final:** As previsões de probabilidade de campeão são salvas na tabela `champ_prediction`, alimentando o Streamlit.
