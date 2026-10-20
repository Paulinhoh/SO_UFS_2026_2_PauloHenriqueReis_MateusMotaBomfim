# GenResearch-Servidor — Atividade 2 (AV2) de Sistemas Operacionais

Serviço simplificado de RAG usado como carga de trabalho para observar memória, armazenamento,
cache de páginas, filas, caches e sincronização em Linux.

## Video
URL: [Sistemas_Operacionais_Atividade02_Grupo08 ](https://www.youtube.com/watch?v=XjiRaoeLrYs)


## Estrutura

| Arquivo | Função |
|---|---|
| `atividade2.py` | Servidor RAG + todos os experimentos (A–E, C1–C6) |
| `gerar_graficos.py` | Gráficos PNG e tabelas CSV/Markdown a partir do JSON |
| `executar_evidencias.sh` | Roda tudo com `vmstat`, `iostat`, `ps -L`, `top -H`, `/usr/bin/time -v` e `strace` |
| `requirements.txt` | Dependências (`sentence-transformers` e `ollama` são opcionais) |
| `dados/` | Corpus sintético JSONL (gerado de forma determinística; índices em `dados/indices/` são regeneráveis) |
| `resultados/` | `relatorio_metricas.json` (dados brutos), `genresearch.log`, `ambiente/`, `evidencias/`, `graficos/` |
| `CONTRIBUICOES.md`, `VIDEO.md`, `RELATORIO_ROTEIRO.md` | Tabela de contribuição, URL do vídeo, mapa do relatório |

## Reprodução

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
ollama pull granite4.1:3b            # LLM usado no projeto

# Execução completa com evidências do SO:
bash executar_evidencias.sh
# ou apenas o experimento + gráficos:
python atividade2.py && python gerar_graficos.py
```

### Perfil rápido com LLM real (~8,5 min, orçamento automático)

```bash
ollama serve &                      # se não estiver rodando
python atividade2.py --perfil rapido --model granite4.1:3b
python gerar_graficos.py
```

Aquece o modelo, mede o tempo real de geração, calcula quantas consultas cabem no orçamento (`--budget-min`, padrão 8,5)
e, se mesmo assim faltar tempo, pula execuções (registradas em `orcamento_tempo` e nos `avisos` do JSON). Declare isso no relatório.

### Outros perfis

```bash
# Rápido, sem LLM e sem embeddings reais (teste de sanidade; NÃO usar como resultado de IA real):
python atividade2.py --skip-llm --embedding-mode hash

# Com LLM real (cada geração leva segundos): reduza consultas e grades
python atividade2.py --consultas 12 --grades g1 g2 --corpus-sizes 300 --repetitions 3 --num-predict 64

# Só alguns experimentos
python atividade2.py --only leitura memoria falhas sincronizacao
```

Principais opções: `--repetitions` (>=3), `--corpus-sizes`, `--cache-sizes`, `--workers`,
`--fila-capacidades`, `--consultas`, `--read-sizes-mb`, `--memory-max-mb`, `--skip-llm`,
`--embedding-mode {auto,real,hash}`, `--only`, `--grades`. Veja `python atividade2.py --help`.

## Mapa do enunciado → código

| Requisito | Onde |
|---|---|
| Parte A (ambiente) | `coletar_ambiente()` → `resultados/ambiente/*.txt` e `A_ambiente` no JSON; `executar_evidencias.sh` |
| B Ingestão, fragmentação, embeddings | `BaseConhecimento.ingerir()` (tempos por etapa, tamanhos de corpus/chunks/índice, E/S) |
| B Armazenamento | `dados/indices/n<N>/{embeddings.npy,chunks.jsonl,meta.json}` |
| B Fila, recuperação, resposta | `executar_run()`, `BaseConhecimento.recuperar()` (contexto limitado em bytes), `Gerador` |
| B Cache | `CacheCompartilhado` (FIFO/LRU, single-flight, tempo economizado, erros nunca cacheados) |
| B Logs | `LogConcorrente` → `resultados/genresearch.log` (escrita protegida por lock) |
| C1/C2 | `exp_leitura()` — convencional x mmap, sequencial x aleatório, fria x quente, 3 tamanhos |
| C3 | grade `g1` (workers 1/2/4 × corpus × cache), 2 produtores; `g3` capacidade da fila |
| C4 | grade `g2` (repetidas/distintas/misto × sem cache/10/50 × FIFO/LRU); `g5` single-flight |
| mmap no pipeline | grade `g4` (`np.load(mmap_mode='r')`) |
| C5 | `exp_pressao_memoria()` — alocação progressiva + escala do índice/lote, com limites seguros |
| C6 | `exp_falhas()` — arquivo ausente, índice indisponível, fila saturada (com/sem retentativa), falha do serviço externo, limite de memória (`RLIMIT_AS`) |
| Parte D | `exp_sincronizacao()` — corrida no cache, contador, log intercalado, buffer limitado com semáforos (empty/full/mutex), consumidor bloqueado, deadlock real em **processo filho isolado** (watchdog + `kill`) e variante com timeout/ordem global |
