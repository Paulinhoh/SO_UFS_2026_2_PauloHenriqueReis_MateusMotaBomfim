# Tabelas geradas automaticamente

Fonte: `resultados/relatorio_metricas.json` (execução 2026-10-18T02:04:58-0300).
- AVISO: RSS pico vem de amostragem periódica (thread); RSS antes/depois não é pico.
- AVISO: Faltas de página (getrusage) são cumulativas do processo; os deltas incluem atividade de outras threads.
- AVISO: O cache de páginas do SO afeta execuções seguintes; veja fria x quente em C1/C2.
- AVISO: Com --skip-llm a geração é SIMULADA (sleep); não representa inferência real.
- AVISO: Com threads, a parte de CPU em Python/NumPy é limitada pelo GIL (parcialmente liberado por NumPy/BLAS); a geração via Ollama é E/S de rede.

### C1/C2 — leitura convencional x mmap (médias das repetições)

| tamanho_MiB | modo | padrao | temperatura | tempo_medio_s | mediana_s | p95_s | rss_pico_delta_MiB | minor_faults | major_faults | io_read_bytes | syscalls_leitura | ops_por_s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | convencional | aleatorio | fria | 0.000148 | 0.0001334 | 0.0001884 | 0 | 1 | 0 | 0 | 112 | 7.833e+05 |
| 1 | convencional | aleatorio | quente | 0.0001313 | 0.0001225 | 0.0001502 | 0 | 1 | 0 | 0 | 112 | 8.615e+05 |
| 1 | convencional | sequencial | fria | 0.0006601 | 0.0005895 | 0.0008146 | 0 | 1 | 0 | 0 | 14 | 2.175e+04 |
| 1 | convencional | sequencial | quente | 0.0006434 | 0.0005724 | 0.0008024 | 0 | 1 | 0 | 0 | 14 | 2.237e+04 |
| 1 | mmap | aleatorio | fria | 0.0002073 | 0.0002129 | 0.0002185 | 0 | 18 | 0 | 0 | 12 | 5.809e+04 |
| 1 | mmap | aleatorio | quente | 0.0001811 | 0.0001798 | 0.0001845 | 0 | 18 | 0 | 0 | 12 | 6.627e+04 |
| 1 | mmap | sequencial | fria | 0.0006105 | 0.0005285 | 0.0007982 | 0.001302 | 18.33 | 0 | 0 | 12 | 2.05e+04 |
| 1 | mmap | sequencial | quente | 0.0005804 | 0.0005098 | 0.0007326 | 0 | 18 | 0 | 0 | 12 | 2.132e+04 |
| 16 | convencional | aleatorio | fria | 0.0006116 | 0.0006014 | 0.0006456 | 0 | 1 | 0 | 0 | 524 | 8.582e+05 |
| 16 | convencional | aleatorio | quente | 0.0005989 | 0.0005886 | 0.0006282 | 0 | 1 | 0 | 0 | 524 | 8.76e+05 |
| 16 | convencional | sequencial | fria | 0.008952 | 0.008891 | 0.009173 | 0.005208 | 1.333 | 0 | 0 | 18 | 2011 |
| 16 | convencional | sequencial | quente | 0.008887 | 0.008814 | 0.009087 | 0.003906 | 1 | 0 | 0 | 18 | 2026 |
| 16 | mmap | aleatorio | fria | 0.001649 | 0.001667 | 0.001678 | 0 | 224 | 0 | 0 | 12 | 7281 |
| 16 | mmap | aleatorio | quente | 0.001607 | 0.001597 | 0.001637 | 0 | 224 | 0 | 0 | 12 | 7470 |
| 16 | mmap | sequencial | fria | 0.006938 | 0.006944 | 0.00695 | 13.48 | 257 | 0 | 0 | 16 | 2306 |
| 16 | mmap | sequencial | quente | 0.006938 | 0.006926 | 0.006991 | 13.44 | 257 | 0 | 0 | 16 | 2306 |
| 128 | convencional | aleatorio | fria | 0.005401 | 0.005564 | 0.005601 | 0.002604 | 1 | 0 | 0 | 4111 | 7.628e+05 |
| 128 | convencional | aleatorio | quente | 0.004947 | 0.004858 | 0.005149 | 0.001302 | 1 | 0 | 0 | 4109 | 8.313e+05 |
| 128 | convencional | sequencial | fria | 0.135 | 0.1348 | 0.1356 | 128 | 3.277e+04 | 0 | 0 | 106 | 785.2 |
| 128 | convencional | sequencial | quente | 0.1356 | 0.1349 | 0.137 | 128 | 32770 | 0 | 0 | 106 | 781.7 |
| 128 | mmap | aleatorio | fria | 0.01251 | 0.01249 | 0.0126 | 106.3 | 1759 | 0 | 0 | 16 | 1279 |
| 128 | mmap | aleatorio | quente | 0.01251 | 0.01249 | 0.01257 | 106.3 | 1759 | 0 | 0 | 16 | 1279 |
| 128 | mmap | sequencial | fria | 0.05399 | 0.05382 | 0.05432 | 119.8 | 2049 | 0 | 0 | 48 | 889.1 |
| 128 | mmap | sequencial | quente | 0.05408 | 0.05408 | 0.05429 | 127.3 | 2049 | 0 | 0 | 48 | 887.6 |

### C3 — concorrência (g1)

| workers | docs | cache | vazao_media | vazao_desvio | lat_media_s | lat_p95_s | espera_fila_media_s | fila_media | fila_max | utilizacao | rss_pico_MiB | perdidos | duplicados | recuperacao_media_s | geracao_media_s | geracao_p95_s | syscalls_por_s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 300 | 10 | 0.162 | 0.0193 | 6.265 | 13.44 | 45.59 | 6.584 | 8 | 1 | 1493 | 0 | 0 | 0.04338 | 9.887 | 13.8 | 195 |
| 2 | 300 | 10 | 0.2119 | 0.01697 | 9.347 | 20.74 | 32.74 | 6.486 | 8 | 0.9833 | 1455 | 0 | 0 | 0.05578 | 14.7 | 20.72 | 194.8 |
| 4 | 300 | 10 | 0.2118 | 0.009387 | 17.98 | 37.51 | 31.71 | 6.54 | 8 | 0.9502 | 1459 | 0 | 0 | 0.06827 | 26.83 | 39.46 | 194.8 |
| 1 | 300 | 50 | 0.224 | 0.01197 | 4.478 | 12.08 | 33.39 | 6.481 | 8 | 1 | 1453 | 0 | 0 | 0.0419 | 7.634 | 12.29 | 194.8 |
| 2 | 300 | 50 | 0.2249 | 0.01156 | 8.799 | 20.76 | 31.8 | 6.551 | 8 | 0.9869 | 1454 | 0 | 0 | 0.05507 | 14.84 | 21.09 | 194.8 |
| 4 | 300 | 50 | 0.226 | 0.01089 | 16.86 | 36.95 | 28.98 | 6.457 | 8 | 0.9505 | 1455 | 0 | 0 | 0.07141 | 27.63 | 38.31 | 194.8 |
| 1 | 1200 | 10 | 0.1771 | 0.03002 | 5.833 | 12.87 | 42.11 | 6.557 | 8 | 1 | 1457 | 0 | 0 | 0.04191 | 8.759 | 12.97 | 194.9 |
| 2 | 1200 | 10 | 0.2315 | 0.01585 | 8.541 | 17.83 | 30.99 | 6.684 | 8 | 0.984 | 1458 | 0 | 0 | 0.0548 | 12.99 | 19.09 | 194.5 |
| 4 | 1200 | 10 | 0.2355 | 0.01975 | 16.27 | 34.68 | 27.92 | 6.513 | 8 | 0.9499 | 1454 | 0 | 0 | 0.07049 | 24.77 | 35.13 | 194.7 |
| 1 | 1200 | 50 | 0.2498 | 0.0152 | 4.019 | 8.837 | 29.82 | 6.615 | 8 | 1 | 1454 | 0 | 0 | 0.04192 | 6.847 | 9.902 | 194.7 |
| 2 | 1200 | 50 | 0.2493 | 0.01551 | 7.888 | 17.42 | 28.62 | 6.51 | 8 | 0.9793 | 1450 | 0 | 0 | 0.05755 | 13.27 | 19.1 | 194.7 |
| 4 | 1200 | 50 | 0.2505 | 0.01462 | 15.19 | 32.83 | 26.49 | 6.293 | 8 | 0.9477 | 1450 | 0 | 0 | 0.07339 | 25.23 | 33.92 | 194.7 |

### C4 — cache (g2)

| cenario | capacidade | politica | taxa_acerto | tempo_economizado_s | substituicoes | vazao_media | tempo_total_s | calculos_duplicados |
|---|---|---|---|---|---|---|---|---|
| repetidas | 0 | lru | 0 | 0 | 0 | 0.195 | 266 | 10.33 |
| repetidas | 10 | fifo | 0.9167 | 330.4 | 0 | 2.292 | 22.33 | 0 |
| repetidas | 10 | lru | 0.9167 | 343.5 | 0 | 2.299 | 22.27 | 0 |
| repetidas | 50 | fifo | 0.9167 | 359.3 | 0 | 2.298 | 22.26 | 0 |
| repetidas | 50 | lru | 0.9167 | 345 | 0 | 2.301 | 22.25 | 0 |
| distintas | 0 | lru | 0 | 0 | 0 | 0.1192 | 407.6 | 0 |
| distintas | 10 | lru | 0 | 0 | 38 | 0.1305 | 368.1 | 0 |
| distintas | 50 | lru | 0 | 0 | 0 | 0.1307 | 367.4 | 0 |
| misto | 0 | lru | 0 | 0 | 0 | 0.1522 | 322.2 | 2.667 |
| misto | 10 | fifo | 0.3194 | 190.9 | 22.67 | 0.2218 | 217.5 | 0 |
| misto | 10 | lru | 0.3611 | 193.1 | 20.67 | 0.2322 | 206.9 | 0 |
| misto | 50 | fifo | 0.4167 | 195.7 | 0 | 0.2516 | 191 | 0 |
| misto | 50 | lru | 0.4167 | 219.6 | 0 | 0.2508 | 191.7 | 0 |

### Índice em memória x mmap (g4)

| indice_mmap | lat_media_s | lat_p95_s | vazao_media | rss_pico_MiB |
|---|---|---|---|---|
| False | 14.98 | 20 | 0.1322 | 1313 |
| True | 15.16 | 19.97 | 0.1307 | 1306 |

### Single-flight x cálculo duplicado (g5)

| single_flight | taxa_acerto | calculos_duplicados | tempo_total_s | vazao_media |
|---|---|---|---|---|
| True | 0.9167 | 0 | 31.5 | 1.644 |
| False | 0.7778 | 6.667 | 59.02 | 0.8577 |

### C5a — alocação progressiva

| alocado_MiB | tempo_passo_s | MiB_por_s | rss_MiB | vsz_MiB | mem_available_MiB | swap_usado_MiB | minor_faults | major_faults |
|---|---|---|---|---|---|---|---|---|
| 64 | 0.05953 | 1075 | 1157 | 9268 | 3037 | 1077 | 11277 | 2 |
| 128 | 0.08452 | 757.2 | 1203 | 9332 | 2874 | 1095 | 11786 | 0 |

### C5b — escala do índice

| fragmentos | matriz_MiB | rss_MiB | lote | tempo_por_consulta_ms | p95_lote_s | minor_faults | major_faults |
|---|---|---|---|---|---|---|---|
| 10000 | 14.65 | 1063 | 1 | 2.542 | 0.01238 | 12 | 0 |
| 10000 | 14.65 | 1063 | 32 | 0.2751 | 0.01972 | 1965 | 114 |
| 50000 | 73.24 | 1059 | 1 | 6.953 | 0.009119 | 1 | 0 |
| 50000 | 73.24 | 1059 | 32 | 1.905 | 0.06825 | 7494 | 26 |

### Parte D — sincronização

| metrica | sem_protecao | com_lock |
|---|---|---|
| cache sem lock — contagens perdidas | 770.7 | 0 |
| cache — tamanho máximo (capacidade 10) | 15 | 10 |
| contador — atualizações perdidas | 445.7 | 0 |
| log — linhas corrompidas | 461.7 | 0 |
