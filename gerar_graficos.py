#!/usr/bin/env python3
"""Gera gráficos (PNG) e tabelas (CSV + Markdown) a partir de resultados/relatorio_metricas.json.

Uso:
    python gerar_graficos.py                                  # lê resultados/relatorio_metricas.json
    python gerar_graficos.py --json caminho.json --saida resultados/graficos
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

CORES = ["#1f77b4", "#d95f02", "#1b9e77", "#7570b3", "#e7298a", "#666666"]


def media(v: list[float]) -> float:
    return statistics.mean(v) if v else float("nan")


def salvar_fig(fig: plt.Figure, caminho: Path) -> None:
    fig.tight_layout()
    fig.savefig(caminho, dpi=150)
    plt.close(fig)
    print("gráfico:", caminho)


def escrever_tabela(pasta: Path, nome: str, cabecalho: list[str], linhas: list[list[Any]], md: list[str], titulo: str) -> None:
    with open(pasta / f"{nome}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(linhas)

    def fmt(x: Any) -> str:
        return f"{x:.4g}" if isinstance(x, float) else str(x)

    md.append(f"\n### {titulo}\n")
    md.append("| " + " | ".join(cabecalho) + " |")
    md.append("|" + "|".join("---" for _ in cabecalho) + "|")
    for l in linhas:
        md.append("| " + " | ".join(fmt(x) for x in l) + " |")


def g_leitura(r: dict[str, Any], pasta: Path, md: list[str]) -> None:
    res = r.get("C1_C2_leitura", {}).get("resumo")
    if not res:
        return
    tamanhos = sorted({x["tamanho_mb"] for x in res})
    combos = [(p, t) for p in ("sequencial", "aleatorio") for t in ("fria", "quente")]
    fig, eixos = plt.subplots(1, 4, figsize=(17, 4), sharey=False)
    for ax, (padrao, temp) in zip(eixos, combos):
        for i, modo in enumerate(("convencional", "mmap")):
            ys = [next((x["tempo_total"]["media_s"] for x in res if x["tamanho_mb"] == t and x["modo"] == modo
                        and x["padrao"] == padrao and x["temperatura"] == temp), float("nan")) for t in tamanhos]
            ax.bar([j + i * 0.38 for j in range(len(tamanhos))], ys, 0.38, label=modo, color=CORES[i])
        ax.set_xticks([j + 0.19 for j in range(len(tamanhos))], [f"{t} MiB" for t in tamanhos])
        ax.set_yscale("log")
        ax.set_title(f"{padrao}, leitura {temp}")
        ax.set_ylabel("tempo total médio (s, escala log)")
    eixos[0].legend()
    salvar_fig(fig, pasta / "c1c2_tempo_leitura.png")

    fig, ax = plt.subplots(figsize=(7, 4))
    for i, modo in enumerate(("convencional", "mmap")):
        ys = [media([x["rss_pico_delta_medio_kb"] / 1024 for x in res if x["tamanho_mb"] == t and x["modo"] == modo
                     and x["padrao"] == "sequencial" and x["rss_pico_delta_medio_kb"] is not None]) for t in tamanhos]
        ax.bar([j + i * 0.38 for j in range(len(tamanhos))], ys, 0.38, label=modo, color=CORES[i])
    ax.set_xticks([j + 0.19 for j in range(len(tamanhos))], [f"{t} MiB" for t in tamanhos])
    ax.set_ylabel("aumento do RSS de pico (MiB)")
    ax.set_title("Leitura sequencial: RSS de pico por modo")
    ax.legend()
    salvar_fig(fig, pasta / "c1c2_rss_pico.png")

    escrever_tabela(pasta, "tabela_c1c2_leitura",
                    ["tamanho_MiB", "modo", "padrao", "temperatura", "tempo_medio_s", "mediana_s", "p95_s", "rss_pico_delta_MiB",
                     "minor_faults", "major_faults", "io_read_bytes", "syscalls_leitura", "ops_por_s"],
                    [[x["tamanho_mb"], x["modo"], x["padrao"], x["temperatura"], x["tempo_total"]["media_s"], x["tempo_total"]["mediana_s"],
                      x["tempo_total"]["p95_s"], (x["rss_pico_delta_medio_kb"] or 0) / 1024, x["minor_faults_medio"],
                      x["major_faults_medio"], x["io_read_bytes_medio"], x["io_syscr_medio"], x["io_ops_por_s_medio"]] for x in res],
                    md, "C1/C2 — leitura convencional x mmap (médias das repetições)")


def g_concorrencia(r: dict[str, Any], pasta: Path, md: list[str]) -> None:
    grades = r.get("C3_C4_concorrencia_cache", {})
    if "g1" in grades:
        resumo = grades["g1"]["resumo"]
        series = sorted({(s["config"]["corpus"], s["config"]["cache_cap"]) for s in resumo})
        fig, eixos = plt.subplots(1, 3, figsize=(16, 4))
        for k, (campo, rot) in enumerate([("vazao_consultas_s", "vazão (consultas/s)"), ("latencia_p95_s", "latência p95 (s)"),
                                          ("utilizacao_workers_media", "utilização média dos workers")]):
            for i, (corpus, cache) in enumerate(series):
                pts = sorted((s["config"]["workers"], s[campo]["media"], s[campo]["desvio"]) for s in resumo
                             if s["config"]["corpus"] == corpus and s["config"]["cache_cap"] == cache)
                eixos[k].errorbar([p[0] for p in pts], [p[1] for p in pts], [p[2] for p in pts], marker="o", capsize=3,
                                  color=CORES[i % len(CORES)], label=f"{corpus} docs, cache {cache}")
            eixos[k].set_xlabel("workers (threads consumidoras)")
            eixos[k].set_ylabel(rot)
            eixos[k].set_xticks(sorted({s["config"]["workers"] for s in resumo}))
        eixos[0].legend(fontsize=8)
        fig.suptitle("C3 — concorrência e fila (barras = desvio padrão entre repetições)")
        salvar_fig(fig, pasta / "c3_concorrencia.png")
        escrever_tabela(pasta, "tabela_c3_concorrencia",
                        ["workers", "docs", "cache", "vazao_media", "vazao_desvio", "lat_media_s", "lat_p95_s", "espera_fila_media_s",
                         "fila_media", "fila_max", "utilizacao", "rss_pico_MiB", "perdidos", "duplicados",
                         "recuperacao_media_s", "geracao_media_s", "geracao_p95_s", "syscalls_por_s"],
                        [[s["config"]["workers"], s["config"]["corpus"], s["config"]["cache_cap"], s["vazao_consultas_s"]["media"],
                          s["vazao_consultas_s"]["desvio"], s["latencia_media_s"]["media"], s["latencia_p95_s"]["media"],
                          s["espera_fila_media_s"]["media"], s["fila_tamanho_medio"]["media"], s["fila_tamanho_maximo"]["media"],
                          s["utilizacao_workers_media"]["media"], (s["rss_pico_kb"]["media"] or 0) / 1024,
                          s["itens_perdidos"]["media"], s["itens_duplicados"]["media"],
                          s["recuperacao_media_s"]["media"], s["geracao_media_s"]["media"], s["geracao_p95_s"]["media"],
                          s["io_syscalls_por_s"]["media"]] for s in sorted(
                            resumo, key=lambda s: (s["config"]["corpus"], s["config"]["cache_cap"], s["config"]["workers"]))],
                        md, "C3 — concorrência (g1)")

    if "g2" in grades:
        resumo = grades["g2"]["resumo"]
        cenarios = ["repetidas", "misto", "distintas"]
        rotulos = sorted({(s["config"]["cache_cap"], s["config"]["politica"]) for s in resumo})
        fig, eixos = plt.subplots(1, 3, figsize=(16, 4))
        for k, (campo, rot) in enumerate([("taxa_acerto", "taxa de acerto"), ("tempo_economizado_s", "tempo economizado (s)"),
                                          ("vazao_consultas_s", "vazão (consultas/s)")]):
            for i, (cap, pol) in enumerate(rotulos):
                ys = [next((s[campo]["media"] for s in resumo if s["config"]["cenario"] == c and s["config"]["cache_cap"] == cap
                            and s["config"]["politica"] == pol), float("nan")) for c in cenarios]
                eixos[k].bar([j + i * 0.8 / len(rotulos) for j in range(len(cenarios))], ys, 0.8 / len(rotulos),
                             color=CORES[i % len(CORES)], label="sem cache" if cap == 0 else f"cache {cap} {pol.upper()}")
            eixos[k].set_xticks([j + 0.4 for j in range(len(cenarios))], cenarios)
            eixos[k].set_ylabel(rot)
        eixos[0].legend(fontsize=8)
        fig.suptitle("C4 — cache: consultas repetidas x distintas")
        salvar_fig(fig, pasta / "c4_cache.png")
        escrever_tabela(pasta, "tabela_c4_cache",
                        ["cenario", "capacidade", "politica", "taxa_acerto", "tempo_economizado_s", "substituicoes", "vazao_media",
                         "tempo_total_s", "calculos_duplicados"],
                        [[s["config"]["cenario"], s["config"]["cache_cap"], s["config"]["politica"], s["taxa_acerto"]["media"],
                          s["tempo_economizado_s"]["media"], s["substituicoes"]["media"], s["vazao_consultas_s"]["media"],
                          s["tempo_total_s"]["media"], s["calculos_duplicados"]["media"]] for s in resumo],
                        md, "C4 — cache (g2)")

    if "g3" in grades:
        resumo = grades["g3"]["resumo"]
        ws = sorted({s["config"]["workers"] for s in resumo})
        fig, eixos = plt.subplots(1, 3, figsize=(16, 4))
        for k, (campo, rot) in enumerate([("bloqueio_produtores_s", "tempo bloqueado dos produtores (s)"),
                                          ("espera_fila_media_s", "espera média na fila (s)"), ("fila_tamanho_medio", "tamanho médio da fila")]):
            for i, w in enumerate(ws):
                pts = sorted((s["config"]["fila_cap"], s[campo]["media"]) for s in resumo if s["config"]["workers"] == w)
                eixos[k].plot([p[0] for p in pts], [p[1] for p in pts], marker="o", color=CORES[i], label=f"{w} worker(s)")
            eixos[k].set_xscale("log")
            eixos[k].set_xlabel("capacidade da fila")
            eixos[k].set_ylabel(rot)
        eixos[0].legend()
        fig.suptitle("Capacidade da fila e contrapressão (g3)")
        salvar_fig(fig, pasta / "c3_fila.png")

    if "g4" in grades:
        escrever_tabela(pasta, "tabela_g4_mmap_indice",
                        ["indice_mmap", "lat_media_s", "lat_p95_s", "vazao_media", "rss_pico_MiB"],
                        [[s["config"]["mmap_indice"], s["latencia_media_s"]["media"], s["latencia_p95_s"]["media"],
                          s["vazao_consultas_s"]["media"], (s["rss_pico_kb"]["media"] or 0) / 1024] for s in grades["g4"]["resumo"]],
                        md, "Índice em memória x mmap (g4)")
    if "g5" in grades:
        escrever_tabela(pasta, "tabela_g5_single_flight",
                        ["single_flight", "taxa_acerto", "calculos_duplicados", "tempo_total_s", "vazao_media"],
                        [[s["config"]["single_flight"], s["taxa_acerto"]["media"], s["calculos_duplicados"]["media"],
                          s["tempo_total_s"]["media"], s["vazao_consultas_s"]["media"]] for s in grades["g5"]["resumo"]],
                        md, "Single-flight x cálculo duplicado (g5)")


def g_memoria(r: dict[str, Any], pasta: Path, md: list[str]) -> None:
    c5 = r.get("C5_pressao_memoria")
    if not c5:
        return
    a = c5.get("C5a_alocacao_progressiva", {})
    passos = a.get("passos", [])
    if passos:
        fig, eixos = plt.subplots(1, 3, figsize=(16, 4))
        x = [p["alocado_mb"] for p in passos]
        eixos[0].plot(x, [p["rss_kb"] / 1024 for p in passos], marker="o", label="RSS", color=CORES[0])
        eixos[0].plot(x, [p["vsz_kb"] / 1024 for p in passos], marker="s", label="VSZ", color=CORES[1])
        eixos[0].set_ylabel("MiB")
        eixos[0].legend()
        eixos[1].plot(x, [(p["mem_available_kb"] or 0) / 1024 for p in passos], marker="o", color=CORES[2], label="MemAvailable")
        eixos[1].plot(x, [(p["swap_usado_kb"] or 0) / 1024 for p in passos], marker="s", color=CORES[4], label="swap usado")
        eixos[1].set_ylabel("MiB")
        eixos[1].legend()
        eixos[2].plot(x, [p["minor_faults_delta"] for p in passos], marker="o", color=CORES[3], label="faltas menores")
        eixos[2].plot(x, [p["major_faults_delta"] for p in passos], marker="s", color=CORES[4], label="faltas maiores")
        eixos[2].set_ylabel("faltas de página por passo")
        eixos[2].legend()
        for ax in eixos:
            ax.set_xlabel("memória alocada (MiB)")
        fig.suptitle(f"C5a — alocação progressiva (parou por: {a.get('parou_por')})")
        salvar_fig(fig, pasta / "c5a_alocacao.png")
        escrever_tabela(pasta, "tabela_c5a_alocacao",
                        ["alocado_MiB", "tempo_passo_s", "MiB_por_s", "rss_MiB", "vsz_MiB", "mem_available_MiB", "swap_usado_MiB",
                         "minor_faults", "major_faults"],
                        [[p["alocado_mb"], p["tempo_passo_s"], p["mb_por_s"], p["rss_kb"] / 1024, p["vsz_kb"] / 1024,
                          (p["mem_available_kb"] or 0) / 1024, (p["swap_usado_kb"] or 0) / 1024,
                          p["minor_faults_delta"], p["major_faults_delta"]] for p in passos], md, "C5a — alocação progressiva")
    esc = [e for e in c5.get("C5b_escala_indice", []) if "lotes" in e]
    if esc:
        fig, eixos = plt.subplots(1, 2, figsize=(11, 4))
        lotes = sorted({l["lote"] for e in esc for l in e["lotes"]})
        for i, lote in enumerate(lotes):
            eixos[0].plot([e["n_chunks"] for e in esc],
                          [next(l["tempo_por_consulta_medio_s"] * 1000 for l in e["lotes"] if l["lote"] == lote) for e in esc],
                          marker="o", color=CORES[i], label=f"lote {lote}")
        eixos[0].set_xscale("log")
        eixos[0].set_yscale("log")
        eixos[0].set_xlabel("fragmentos no índice")
        eixos[0].set_ylabel("tempo por consulta (ms)")
        eixos[0].legend()
        eixos[1].plot([e["n_chunks"] for e in esc], [e["tamanho_matriz_mb"] for e in esc], marker="o", label="matriz de embeddings", color=CORES[0])
        eixos[1].plot([e["n_chunks"] for e in esc], [e["rss_kb"] / 1024 for e in esc], marker="s", label="RSS do processo", color=CORES[1])
        eixos[1].set_xscale("log")
        eixos[1].set_xlabel("fragmentos no índice")
        eixos[1].set_ylabel("MiB")
        eixos[1].legend()
        fig.suptitle("C5b — crescimento do índice de embeddings e do lote de consultas")
        salvar_fig(fig, pasta / "c5b_escala_indice.png")
        escrever_tabela(pasta, "tabela_c5b_escala",
                        ["fragmentos", "matriz_MiB", "rss_MiB", "lote", "tempo_por_consulta_ms", "p95_lote_s", "minor_faults", "major_faults"],
                        [[e["n_chunks"], e["tamanho_matriz_mb"], e["rss_kb"] / 1024, l["lote"], l["tempo_por_consulta_medio_s"] * 1000,
                          l["tempo_lote"]["p95_s"], l["minor_faults_delta"], l["major_faults_delta"]] for e in esc for l in e["lotes"]],
                        md, "C5b — escala do índice")


def g_sincronizacao(r: dict[str, Any], pasta: Path, md: list[str]) -> None:
    d = r.get("D_sincronizacao")
    if not d:
        return
    cache, cont, log = d["D2_cache_compartilhado_corrida"], d["D_contador_atualizacao_perdida"], d["D_log_intercalado"]
    linhas = [
        ["cache sem lock — contagens perdidas", media([x["contagens_perdidas"] for x in cache["sem_lock"]]), media([x["contagens_perdidas"] for x in cache["com_lock"]])],
        ["cache — tamanho máximo (capacidade 10)", media([x["tamanho_maximo_observado"] for x in cache["sem_lock"]]), media([x["tamanho_maximo_observado"] for x in cache["com_lock"]])],
        ["contador — atualizações perdidas", media([x["atualizacoes_perdidas"] for x in cont["inseguro"]]), media([x["atualizacoes_perdidas"] for x in cont["seguro"]])],
        ["log — linhas corrompidas", media([x["linhas_corrompidas"] for x in log["sem_lock"]]), media([x["linhas_corrompidas"] for x in log["com_lock"]])],
    ]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.barh([l[0] for l in linhas], [l[1] for l in linhas], 0.4, label="sem proteção", color=CORES[1])
    ax.barh([j + 0.4 for j in range(len(linhas))], [l[2] for l in linhas], 0.4, label="com lock", color=CORES[2])
    ax.set_yticks([j + 0.2 for j in range(len(linhas))], [l[0] for l in linhas])
    ax.invert_yaxis()
    ax.legend()
    ax.set_title("Parte D — falhas de sincronização x correção (média das repetições)")
    salvar_fig(fig, pasta / "d_sincronizacao.png")
    escrever_tabela(pasta, "tabela_d_sincronizacao", ["metrica", "sem_protecao", "com_lock"], linhas, md, "Parte D — sincronização")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--json", default="resultados/relatorio_metricas.json")
    p.add_argument("--saida", default="resultados/graficos")
    a = p.parse_args()
    r = json.load(open(a.json, encoding="utf-8"))
    pasta = Path(a.saida)
    pasta.mkdir(parents=True, exist_ok=True)
    md: list[str] = ["# Tabelas geradas automaticamente", f"\nFonte: `{a.json}` (execução {r.get('inicio_execucao')})."]
    for av in r.get("avisos", []):
        md.append(f"- AVISO: {av}")
    for f in (g_leitura, g_concorrencia, g_memoria, g_sincronizacao):
        f(r, pasta, md)
    (pasta / "tabelas.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("tabelas:", pasta / "tabelas.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
