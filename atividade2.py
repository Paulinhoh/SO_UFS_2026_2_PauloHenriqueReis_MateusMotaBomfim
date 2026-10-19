#!/usr/bin/env python3
"""
GenResearch-Servidor — Atividade 2 (AV2) de Sistemas Operacionais.

Cobre as partes A–E do enunciado:

  A  Observação do ambiente         -> coleta de comandos do SO (uname, lscpu, free, ...) + git
  B  Servidor simplificado de RAG   -> ingestão (fragmentação + embeddings), índice persistido em
                                        disco, fila, cache compartilhado, recuperação, geração e
                                        log em arquivo com escrita protegida
  C  Experimentos C1..C6            -> leitura convencional x mmap (fria/quente, sequencial/aleatória),
                                        concorrência e fila, cache (repetidas x distintas, FIFO x LRU,
                                        single-flight), pressão de memória, falhas controladas
  D  Sincronização                  -> condição de corrida no cache e no contador, log intercalado,
                                        buffer limitado com semáforos (empty/full/mutex), consumidor
                                        bloqueado, espera circular (deadlock) isolada com timeout
  E  Memória virtual / armazenamento -> RSS pico, VSZ, MemAvailable, swap, faltas de página, E/S

Dependências para RAG real (opcionais; sem elas o programa usa embeddings simulados e avisa):
    pip install -r requirements.txt
    ollama pull granite4.1:3b

Exemplos:
    python atividade2.py                         # tudo, com embeddings/LLM reais se disponíveis
    python atividade2.py --skip-llm              # LLM simulado (NÃO mede inferência real)
    python atividade2.py --only leitura falhas   # apenas alguns experimentos

Regra de honestidade dos dados: o modo usado (real, simulado, falha) é sempre registrado
no relatório JSON e no log; respostas simuladas nunca são contadas como inferência real.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import json
import mmap
import os
import platform
import queue
import random
import re
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zlib
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable

import numpy as np

try:
    from sentence_transformers import SentenceTransformer
except ImportError:  # dependência opcional
    SentenceTransformer = None

try:
    import ollama
except ImportError:  # dependência opcional
    ollama = None


DEFAULT_MODEL = "granite4.1:3b"
DEFAULT_EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
MiB = 1024 * 1024
EXPERIMENTOS = ["ambiente", "leitura", "concorrencia", "memoria", "falhas", "sincronizacao"]
GRADES = ["g1", "g2", "g3", "g4", "g5"]

# --------------------------------------------------------------------------------------
# Utilidades de medição
# --------------------------------------------------------------------------------------


def agora() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def ler_meminfo() -> dict[str, int]:
    """/proc/meminfo em KiB (vazio fora do Linux)."""
    info: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                partes = linha.split()
                if len(partes) >= 2:
                    info[partes[0].rstrip(":")] = int(partes[1])
    except (OSError, ValueError):
        pass
    return info


def ler_status_proc() -> dict[str, int | None]:
    """VmSize (VSZ), VmRSS, VmHWM (pico de RSS do processo) e Threads de /proc/self/status."""
    campos = {"VmSize": "vsz_kb", "VmRSS": "rss_kb", "VmHWM": "rss_pico_proc_kb", "Threads": "threads"}
    resultado: dict[str, int | None] = {v: None for v in campos.values()}
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                chave = linha.split(":")[0]
                if chave in campos:
                    resultado[campos[chave]] = int(linha.split()[1])
    except (OSError, ValueError):
        pass
    return resultado


def ler_io_proc() -> dict[str, int]:
    """Contadores de E/S do processo (rchar/wchar = chamadas; read_bytes/write_bytes = disco)."""
    resultado: dict[str, int] = {}
    try:
        with open("/proc/self/io", "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                chave, _, valor = linha.partition(":")
                resultado[chave.strip()] = int(valor)
    except (OSError, ValueError):
        pass
    return resultado


def faltas_pagina() -> dict[str, int | None]:
    try:
        uso = resource.getrusage(resource.RUSAGE_SELF)
        return {"minor_faults": int(uso.ru_minflt), "major_faults": int(uso.ru_majflt)}
    except (AttributeError, OSError):
        return {"minor_faults": None, "major_faults": None}


def diferenca(antes: dict[str, Any], depois: dict[str, Any]) -> dict[str, Any]:
    return {
        chave: depois[chave] - antes[chave]
        for chave in depois
        if chave in antes and antes[chave] is not None and depois[chave] is not None
    }


def resumo_tempos(valores: list[float]) -> dict[str, float | int | None]:
    if not valores:
        return {"n": 0, "media_s": None, "mediana_s": None, "p95_s": None, "min_s": None, "max_s": None}
    ordenados = sorted(valores)
    indice_p95 = max(0, min(len(ordenados) - 1, int(np.ceil(0.95 * len(ordenados))) - 1))
    return {
        "n": len(valores),
        "media_s": statistics.mean(valores),
        "mediana_s": statistics.median(valores),
        "p95_s": ordenados[indice_p95],
        "min_s": ordenados[0],
        "max_s": ordenados[-1],
    }


class AmostradorRecursos:
    """Thread que amostra RSS/VSZ/MemAvailable/Swap/Cached do sistema a cada `intervalo` s.

    Serve para medir PICO (o enunciado exige mais que RSS antes/depois). O amostrador é uma
    thread extra e disputa o GIL de forma desprezível, mas ela existe: está documentada.
    """

    def __init__(self, intervalo: float = 0.02):
        self.intervalo = intervalo
        self.amostras: list[dict[str, Any]] = []
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None

    def _amostrar(self) -> None:
        st = ler_status_proc()
        mi = ler_meminfo()
        self.amostras.append({
            "t": time.perf_counter(),
            "rss_kb": st["rss_kb"],
            "vsz_kb": st["vsz_kb"],
            "mem_available_kb": mi.get("MemAvailable"),
            "cached_kb": mi.get("Cached"),
            "swap_usado_kb": (mi["SwapTotal"] - mi["SwapFree"]) if "SwapTotal" in mi and "SwapFree" in mi else None,
        })

    def _loop(self) -> None:
        while not self._parar.wait(self.intervalo):
            self._amostrar()

    def __enter__(self) -> "AmostradorRecursos":
        self._amostrar()
        self._thread = threading.Thread(target=self._loop, name="amostrador-recursos", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self._parar.set()
        if self._thread:
            self._thread.join()
        self._amostrar()

    def resumo(self) -> dict[str, Any]:
        def serie(chave: str) -> list[int]:
            return [a[chave] for a in self.amostras if a[chave] is not None]

        rss, vsz = serie("rss_kb"), serie("vsz_kb")
        disp, cache, swap = serie("mem_available_kb"), serie("cached_kb"), serie("swap_usado_kb")
        return {
            "amostras": len(self.amostras),
            "rss_inicial_kb": rss[0] if rss else None,
            "rss_pico_kb": max(rss) if rss else None,
            "vsz_pico_kb": max(vsz) if vsz else None,
            "mem_available_min_kb": min(disp) if disp else None,
            "cached_max_kb": max(cache) if cache else None,
            "swap_usado_max_kb": max(swap) if swap else None,
        }


class LogConcorrente:
    """Log em arquivo com escrita protegida por lock (uma linha = uma chamada de write)."""

    def __init__(self, caminho: Path | None):
        self.lock = threading.Lock()
        self.arquivo = open(caminho, "a", encoding="utf-8", buffering=1) if caminho else None
        self.linhas = 0

    def evento(self, evento: str, **dados: Any) -> None:
        t = time.time()
        carimbo = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(t)) + f".{int((t % 1) * 1000):03d}"
        linha = (
            f"{carimbo} | {threading.current_thread().name} | {evento} | "
            f"{json.dumps(dados, ensure_ascii=False, default=str)}\n"
        )
        with self.lock:
            self.linhas += 1
            if self.arquivo:
                self.arquivo.write(linha)

    def fechar(self) -> None:
        with self.lock:
            if self.arquivo:
                self.arquivo.flush()
                self.arquivo.close()
                self.arquivo = None


# --------------------------------------------------------------------------------------
# Parte A — ambiente
# --------------------------------------------------------------------------------------


def coletar_ambiente(args: argparse.Namespace, pasta: Path, modo_embedding: str, nome_embedding: str) -> dict[str, Any]:
    pasta.mkdir(parents=True, exist_ok=True)
    comandos: dict[str, list[str]] = {
        "uname": ["uname", "-a"],
        "os_release": ["cat", "/etc/os-release"],
        "lscpu": ["lscpu"],
        "free": ["free", "-h"],
        "swapon": ["swapon", "--show"],
        "lsblk": ["lsblk"],
        "df": ["df", "-h"],
        "findmnt_dados": ["findmnt", "-T", str(Path(args.output_dir).resolve())],
        "nvidia_smi": ["nvidia-smi"],
        "git_commit": ["git", "rev-parse", "HEAD"],
        "git_status": ["git", "status", "--short"],
        "pip_freeze": [sys.executable, "-m", "pip", "freeze"],
        "ollama_version": ["ollama", "--version"],
        "ollama_list": ["ollama", "list"],
        "vmstat_amostra": ["vmstat", "1", "3"],
        "iostat_amostra": ["iostat", "-xz", "1", "2"],
    }
    saidas: dict[str, Any] = {}
    for nome, cmd in comandos.items():
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            texto = (proc.stdout or "") + (("\n[stderr]\n" + proc.stderr) if proc.stderr.strip() else "")
            (pasta / f"{nome}.txt").write_text(texto, encoding="utf-8")
            saidas[nome] = {"codigo_saida": proc.returncode, "saida": texto[:3000]}
        except FileNotFoundError:
            saidas[nome] = {"codigo_saida": None, "saida": "comando indisponível neste ambiente"}
        except subprocess.TimeoutExpired:
            saidas[nome] = {"codigo_saida": None, "saida": "timeout"}
    meminfo = ler_meminfo()
    (pasta / "proc_meminfo.txt").write_text(
        "\n".join(f"{k}: {v} kB" for k, v in meminfo.items()), encoding="utf-8"
    )
    return {
        "data_execucao": agora(),
        "plataforma": platform.platform(),
        "kernel": platform.release(),
        "python": sys.version,
        "cpu_logicas": os.cpu_count(),
        "page_size_bytes": mmap.PAGESIZE,
        "meminfo_selecionado_kb": {
            k: meminfo.get(k)
            for k in ["MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached", "SwapTotal", "SwapFree", "Dirty"]
        },
        "modelo_llm": args.model,
        "llm_modo": "simulado (--skip-llm)" if args.skip_llm else ("ollama" if ollama else "indisponível (pacote ollama ausente)"),
        "modelo_embedding": nome_embedding,
        "embedding_modo": modo_embedding,
        "dependencias": {
            "numpy": np.__version__,
            "sentence_transformers_disponivel": SentenceTransformer is not None,
            "ollama_python_disponivel": ollama is not None,
        },
        "comandos": saidas,
        "observacao": "Saídas completas em resultados/ambiente/*.txt. Registre GPU/VRAM manualmente se nvidia-smi não existir.",
    }


# --------------------------------------------------------------------------------------
# Embeddings, corpus, fragmentação e base de conhecimento
# --------------------------------------------------------------------------------------


class EmbedderHash:
    """Embeddings SIMULADOS por feature hashing de unigramas e bigramas (determinístico, sem GPU)."""

    modo = "simulado_hash"

    def __init__(self, dim: int = 384):
        self.dim = dim
        self.nome = f"hash-bow-{dim}"

    def encode(self, textos: list[str]) -> np.ndarray:
        matriz = np.zeros((len(textos), self.dim), dtype=np.float32)
        for i, texto in enumerate(textos):
            tokens = re.findall(r"\w+", texto.lower())
            for feat in tokens + [a + "_" + b for a, b in zip(tokens, tokens[1:])]:
                h = zlib.crc32(feat.encode("utf-8"))
                matriz[i, h % self.dim] += 1.0 if (h >> 16) & 1 else -1.0
        normas = np.linalg.norm(matriz, axis=1, keepdims=True)
        normas[normas == 0] = 1.0
        return matriz / normas


class EmbedderReal:
    modo = "real"

    def __init__(self, nome: str):
        self.nome = nome
        self.modelo = SentenceTransformer(nome)
        self.dim = int(self.modelo.get_sentence_embedding_dimension())

    def encode(self, textos: list[str]) -> np.ndarray:
        return self.modelo.encode(
            textos, show_progress_bar=False, normalize_embeddings=True, convert_to_numpy=True
        ).astype(np.float32)


def criar_embedder(modo: str, nome: str) -> tuple[Any, str | None]:
    if modo in ("auto", "real"):
        if SentenceTransformer is not None:
            return EmbedderReal(nome), None
        if modo == "real":
            raise RuntimeError("--embedding-mode real exige: pip install sentence-transformers")
        return EmbedderHash(), "sentence-transformers ausente: usando embeddings SIMULADOS (hash). Não apresente como embeddings reais."
    return EmbedderHash(), "embeddings simulados (hash) por escolha explícita."


TOPICOS: dict[str, list[str]] = {
    "paginação": [
        "A paginação divide o espaço de endereços virtual em páginas de tamanho fixo mapeadas para quadros da memória física.",
        "Uma falta de página ocorre quando o processo acessa uma página sem mapeamento residente, e o kernel precisa resolvê-la.",
        "Faltas maiores exigem leitura em disco ou swap, enquanto faltas menores são resolvidas apenas com a memória já presente.",
    ],
    "mmap": [
        "A chamada mmap mapeia um arquivo no espaço de endereços do processo e carrega as páginas sob demanda.",
        "Com mmap o acesso ao arquivo evita a cópia explícita do buffer do kernel para o espaço do usuário.",
        "Páginas de arquivos mapeados compartilham o cache de páginas e podem ser descartadas pelo kernel sob pressão de memória.",
    ],
    "cache de páginas": [
        "O cache de páginas mantém em RAM blocos de arquivos lidos recentemente para acelerar acessos futuros.",
        "Memória usada como cache não é desperdiçada, pois o kernel a devolve rapidamente quando um processo precisa dela.",
        "A primeira leitura de um arquivo costuma ser fria e as seguintes quentes, com latência muito menor.",
    ],
    "swap": [
        "O swap estende a memória virtual usando armazenamento secundário para páginas anônimas pouco utilizadas.",
        "Uso intenso de swap eleva a latência, porque o disco é ordens de grandeza mais lento que a RAM.",
        "O parâmetro swappiness controla a tendência do kernel de mover páginas anônimas para o swap.",
    ],
    "fila bloqueante": [
        "Uma fila bloqueante com capacidade limitada coordena produtores e consumidores e impõe contrapressão.",
        "Quando a fila está cheia o produtor espera, e quando está vazia o consumidor espera sem consumir CPU.",
        "A capacidade da fila controla o compromisso entre vazão, latência de espera e consumo de memória.",
    ],
    "mutex": [
        "Um mutex garante exclusão mútua em uma região crítica contra atualizações concorrentes inconsistentes.",
        "Sem sincronização, a sequência ler-modificar-escrever pode perder atualizações quando duas threads se intercalam.",
        "Locks protegem a consistência, mas introduzem contenção e podem causar deadlock se adquiridos em ordens diferentes.",
    ],
    "RSS e VSZ": [
        "O RSS é a parte da memória do processo que está de fato residente na RAM naquele instante.",
        "O VSZ é o tamanho total do espaço de endereços virtual reservado, residente ou não.",
        "VSZ grande não implica pressão de RAM, mas um RSS crescente sinaliza consumo real de memória física.",
    ],
    "latência p95": [
        "A latência p95 é o valor abaixo do qual ficam cerca de noventa e cinco por cento das requisições.",
        "Médias escondem caudas longas, por isso percentis são mais informativos para serviços interativos.",
        "Contenção em filas e locks costuma inflar o p95 antes de afetar visivelmente a média.",
    ],
    "RAG": [
        "A arquitetura RAG recupera trechos relevantes de uma base e os inclui no prompt de um modelo generativo.",
        "O contexto recuperado é limitado em bytes ou tokens, pois contextos longos aumentam a latência e a memória.",
        "A etapa de geração costuma dominar o tempo total de uma consulta RAG em modelos locais.",
    ],
    "embeddings": [
        "Embeddings são vetores densos que representam textos de forma que a similaridade de cosseno reflita proximidade de sentido.",
        "A matriz de embeddings cresce linearmente com o número de fragmentos e precisa caber na memória ou ser mapeada.",
        "Normalizar os vetores permite calcular a similaridade de cosseno por um simples produto escalar.",
    ],
    "checkpoints": [
        "Checkpoints de modelos são arquivos grandes carregados do armazenamento para a memória antes da inferência.",
        "Carregar pesos por mmap permite iniciar mais rápido, pois as páginas são lidas somente quando acessadas.",
        "O cache de páginas faz o segundo carregamento do mesmo checkpoint ser muito mais rápido que o primeiro.",
    ],
    "thrashing": [
        "Thrashing ocorre quando o sistema gasta mais tempo paginando do que executando trabalho útil.",
        "Conjuntos de trabalho maiores que a RAM disponível provocam faltas de página em excesso e queda de desempenho.",
        "Limitar a concorrência e o tamanho do lote reduz o conjunto de trabalho e ajuda a evitar thrashing.",
    ],
}
ASPECTOS = [
    "O que é", "Como funciona", "Qual o custo de", "Quando ocorre problema com", "Como medir", "Qual a relação com desempenho de",
]


def gerar_corpus(caminho: Path, n_docs: int, seed: int = 42) -> Path:
    """Corpus sintético determinístico em JSONL (um documento por linha)."""
    rng = random.Random(seed)
    nomes = list(TOPICOS)
    todas = [frase for lista in TOPICOS.values() for frase in lista]
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as arquivo:
        for i in range(n_docs):
            topico = nomes[i % len(nomes)]
            frases = [
                rng.choice(TOPICOS[topico]) if rng.random() < 0.7 else rng.choice(todas)
                for _ in range(rng.randint(10, 18))
            ]
            frases.append(f"Anotação {i + 1}: valor de referência {rng.randint(100, 999)} no experimento {rng.choice('ABCD')}.")
            doc = {"id": i + 1, "titulo": f"Documento {i + 1} sobre {topico}", "topico": topico, "texto": " ".join(frases)}
            arquivo.write(json.dumps(doc, ensure_ascii=False) + "\n")
    return caminho


def fragmentar(texto: str, palavras: int = 60, sobreposicao: int = 10) -> list[str]:
    tokens = texto.split()
    passo = max(1, palavras - sobreposicao)
    pedacos = []
    for inicio in range(0, len(tokens), passo):
        pedacos.append(" ".join(tokens[inicio:inicio + palavras]))
        if inicio + palavras >= len(tokens):
            break
    return pedacos


class BaseConhecimento:
    """Documentos -> fragmentos -> embeddings persistidos (JSONL + .npy). Recuperação por cosseno."""

    def __init__(self, embedder: Any, logger: LogConcorrente, top_k: int = 3, max_ctx_bytes: int = 1500):
        self.embedder = embedder
        self.logger = logger
        self.top_k = top_k
        self.max_ctx_bytes = max_ctx_bytes
        self.chunks: list[str] = []
        self.embeddings: np.ndarray | None = None
        self.usa_mmap = False

    def ingerir(self, corpus_jsonl: Path, pasta_indice: Path, palavras: int = 60, sobreposicao: int = 10) -> dict[str, Any]:
        self.logger.evento("ingestao_inicio", corpus=str(corpus_jsonl))
        io_antes = ler_io_proc()
        t0 = time.perf_counter()
        with open(corpus_jsonl, "r", encoding="utf-8") as arquivo:  # FileNotFoundError propaga (C6)
            docs = [json.loads(linha) for linha in arquivo if linha.strip()]
        if not docs:
            raise ValueError("O corpus está vazio.")
        t_leitura = time.perf_counter() - t0

        t1 = time.perf_counter()
        chunks, meta = [], []
        for doc in docs:
            for j, pedaco in enumerate(fragmentar(doc["texto"], palavras, sobreposicao)):
                chunks.append(pedaco)
                meta.append({"doc_id": doc["id"], "chunk": j, "topico": doc.get("topico")})
        t_frag = time.perf_counter() - t1

        t2 = time.perf_counter()
        lotes = [self.embedder.encode(chunks[i:i + 256]) for i in range(0, len(chunks), 256)]
        embeddings = np.vstack(lotes).astype(np.float32)
        t_emb = time.perf_counter() - t2

        t3 = time.perf_counter()
        pasta_indice.mkdir(parents=True, exist_ok=True)
        np.save(pasta_indice / "embeddings.npy", embeddings)
        with open(pasta_indice / "chunks.jsonl", "w", encoding="utf-8") as arquivo:
            for texto, m in zip(chunks, meta):
                arquivo.write(json.dumps({"texto": texto, **m}, ensure_ascii=False) + "\n")
        (pasta_indice / "meta.json").write_text(
            json.dumps({"embedder": self.embedder.nome, "modo": self.embedder.modo, "dim": int(embeddings.shape[1]),
                        "chunks": len(chunks), "palavras": palavras, "sobreposicao": sobreposicao}),
            encoding="utf-8",
        )
        t_persist = time.perf_counter() - t3

        self.chunks = chunks
        self.embeddings = embeddings
        self.usa_mmap = False
        io_depois = ler_io_proc()
        resultado = {
            "documentos": len(docs),
            "chunks": len(chunks),
            "embedding_shape": list(embeddings.shape),
            "embedding_modo": self.embedder.modo,
            "tempo_leitura_s": t_leitura,
            "tempo_fragmentacao_s": t_frag,
            "tempo_embeddings_s": t_emb,
            "tempo_persistencia_s": t_persist,
            "tempo_ingestao_total_s": time.perf_counter() - t0,
            "tamanho_corpus_bytes": os.path.getsize(corpus_jsonl),
            "tamanho_chunks_bytes": os.path.getsize(pasta_indice / "chunks.jsonl"),
            "tamanho_indice_embeddings_bytes": os.path.getsize(pasta_indice / "embeddings.npy"),
            "io_delta": diferenca(io_antes, io_depois),
        }
        self.logger.evento("ingestao_fim", **{k: resultado[k] for k in ("documentos", "chunks", "tempo_ingestao_total_s")})
        return resultado

    def carregar(self, pasta_indice: Path, usar_mmap: bool) -> dict[str, Any]:
        t0 = time.perf_counter()
        arq_emb = pasta_indice / "embeddings.npy"
        if not arq_emb.exists():
            raise FileNotFoundError(f"índice indisponível: {arq_emb}")
        self.embeddings = np.load(arq_emb, mmap_mode="r" if usar_mmap else None)
        with open(pasta_indice / "chunks.jsonl", "r", encoding="utf-8") as arquivo:
            self.chunks = [json.loads(linha)["texto"] for linha in arquivo]
        self.usa_mmap = usar_mmap
        self.logger.evento("indice_carregado", pasta=str(pasta_indice), mmap=usar_mmap, chunks=len(self.chunks))
        return {"tempo_carga_s": time.perf_counter() - t0, "mmap": usar_mmap, "chunks": len(self.chunks)}

    def recuperar(self, consulta: str) -> dict[str, Any]:
        if self.embeddings is None or not self.chunks:
            raise RuntimeError("É necessário ingerir ou carregar um índice antes de consultar.")
        t0 = time.perf_counter()
        vetor = self.embedder.encode([consulta])[0]
        sim = self.embeddings @ vetor  # vetores normalizados: produto escalar = cosseno
        k = min(self.top_k, len(self.chunks))
        idx = np.argpartition(-sim, k - 1)[:k]
        idx = idx[np.argsort(-sim[idx])]
        escolhidos, usados = [], 0
        for i in idx:
            texto = self.chunks[int(i)]
            tam = len(texto.encode("utf-8"))
            if escolhidos and usados + tam > self.max_ctx_bytes:
                break
            escolhidos.append(texto)
            usados += tam
        contexto = "\n".join(escolhidos)
        return {
            "contexto": contexto,
            "n_trechos": len(escolhidos),
            "bytes_contexto_utf8": len(contexto.encode("utf-8")),
            "similaridade_max": float(sim[idx[0]]),
            "tempo_recuperacao_s": time.perf_counter() - t0,
        }


# --------------------------------------------------------------------------------------
# Geração (LLM real, simulada ou falha — sempre rotulada) com retentativas
# --------------------------------------------------------------------------------------


class Gerador:
    def __init__(self, modelo: str, usar_llm: bool, sim_gen_ms: float, timeout_s: float, num_predict: int,
                 max_tentativas: int, backoff_s: float, logger: LogConcorrente,
                 injetor_falha: Callable[[str, int], bool] | None = None):
        self.modelo, self.usar_llm, self.sim_gen_s = modelo, usar_llm, sim_gen_ms / 1000.0
        self.num_predict, self.max_tentativas, self.backoff_s = num_predict, max_tentativas, backoff_s
        self.logger, self.injetor_falha = logger, injetor_falha
        self.cliente = ollama.Client(timeout=timeout_s) if (usar_llm and ollama is not None) else None

    def _uma_tentativa(self, consulta: str, contexto: str, tentativa: int) -> tuple[str, str]:
        if self.injetor_falha and self.injetor_falha(consulta, tentativa):
            raise ConnectionError("falha injetada para teste de robustez (C6)")
        if not self.usar_llm:
            time.sleep(self.sim_gen_s)  # custo simulado, bloqueante como uma chamada de rede
            return f"[SIMULADO] resposta baseada em: {contexto[:100]}", "simulado"
        if self.cliente is None:
            raise RuntimeError("pacote Python 'ollama' não instalado")
        prompt = (
            "Responda em português, de forma objetiva, usando somente o contexto. "
            "Se o contexto não contiver a resposta, diga isso explicitamente.\n\n"
            f"Contexto:\n{contexto}\n\nConsulta: {consulta}"
        )
        resultado = self.cliente.generate(model=self.modelo, prompt=prompt, keep_alive="15m",
                                          options={"num_predict": self.num_predict, "temperature": 0})
        try:
            texto = resultado.response
        except AttributeError:
            texto = resultado["response"]
        return texto, "ollama_real"

    def gerar(self, consulta: str, contexto: str) -> dict[str, Any]:
        inicio = time.perf_counter()
        ultimo_erro = None
        for tentativa in range(1, self.max_tentativas + 1):
            try:
                texto, modo = self._uma_tentativa(consulta, contexto, tentativa)
                return {"resposta": texto, "modo_resposta": modo, "erro": None, "tentativas": tentativa,
                        "tempo_geracao_s": time.perf_counter() - inicio}
            except Exception as exc:
                ultimo_erro = f"{type(exc).__name__}: {exc}"
                self.logger.evento("geracao_falha", tentativa=tentativa, erro=ultimo_erro)
                if isinstance(exc, RuntimeError) or tentativa == self.max_tentativas:
                    break
                time.sleep(self.backoff_s * (2 ** (tentativa - 1)))
        return {"resposta": "", "modo_resposta": "falha", "erro": ultimo_erro, "tentativas": tentativa,
                "tempo_geracao_s": time.perf_counter() - inicio}


# --------------------------------------------------------------------------------------
# Cache compartilhado (FIFO/LRU, single-flight, métricas, tempo economizado)
# --------------------------------------------------------------------------------------


class CacheCompartilhado:
    """Cache thread-safe. capacidade=0 desativa o cache (linha de base).

    single_flight=True: se duas threads pedem a mesma chave ao mesmo tempo, apenas uma calcula
    e as demais esperam (evita trabalho duplicado). Respostas com erro NUNCA são cacheadas.
    """

    def __init__(self, capacidade: int, politica: str = "lru", single_flight: bool = True):
        if capacidade < 0:
            raise ValueError("capacidade deve ser >= 0")
        if politica not in ("fifo", "lru"):
            raise ValueError("política deve ser 'fifo' ou 'lru'")
        self.capacidade, self.politica, self.single_flight = capacidade, politica, single_flight
        self.dados: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.lock = threading.Lock()
        self.em_voo: dict[str, threading.Event] = {}
        self.calculando: dict[str, int] = {}
        self.acertos = self.erros = self.substituicoes = 0
        self.calculos_duplicados = self.coalescidos = self.nao_cacheados_por_erro = 0
        self.tempo_economizado_s = 0.0

    def _inserir(self, chave: str, resposta: dict[str, Any], custo: float) -> None:
        if chave in self.dados:
            self.dados[chave] = {"resposta": dict(resposta), "custo_s": custo}
            if self.politica == "lru":
                self.dados.move_to_end(chave)
            return
        if len(self.dados) >= self.capacidade:
            self.dados.popitem(last=False)
            self.substituicoes += 1
        self.dados[chave] = {"resposta": dict(resposta), "custo_s": custo}

    def obter_ou_calcular(self, chave: str, calcular: Callable[[], tuple[dict[str, Any], float]]
                          ) -> tuple[dict[str, Any], bool, float]:
        """Retorna (resposta, hit, custo_s). `custo_s` é o custo de calcular (real se miss; evitado se hit)."""
        usa_voo = self.single_flight and self.capacidade > 0
        while True:
            with self.lock:
                entrada = self.dados.get(chave)
                if entrada is not None:
                    self.acertos += 1
                    self.tempo_economizado_s += entrada["custo_s"]
                    if self.politica == "lru":
                        self.dados.move_to_end(chave)
                    return dict(entrada["resposta"]), True, entrada["custo_s"]
                if usa_voo and chave in self.em_voo:
                    evento = self.em_voo[chave]
                    self.coalescidos += 1
                else:
                    evento = None
                    self.erros += 1
                    if self.calculando.get(chave, 0) > 0:
                        self.calculos_duplicados += 1
                    self.calculando[chave] = self.calculando.get(chave, 0) + 1
                    if usa_voo:
                        self.em_voo[chave] = threading.Event()
            if evento is None:
                break
            evento.wait()
        try:
            resposta, custo = calcular()
        except BaseException:
            with self.lock:
                self.calculando[chave] -= 1
                ev = self.em_voo.pop(chave, None) if usa_voo else None
            if ev:
                ev.set()
            raise
        with self.lock:
            self.calculando[chave] -= 1
            if resposta.get("erro"):
                self.nao_cacheados_por_erro += 1
            elif self.capacidade > 0:
                self._inserir(chave, resposta, custo)
            ev = self.em_voo.pop(chave, None) if usa_voo else None
        if ev:
            ev.set()
        return resposta, False, custo

    def metricas(self) -> dict[str, Any]:
        with self.lock:
            total = self.acertos + self.erros
            return {
                "capacidade": self.capacidade, "politica": self.politica, "single_flight": self.single_flight,
                "itens_atuais": len(self.dados), "acertos": self.acertos, "erros": self.erros,
                "taxa_acerto": self.acertos / total if total else 0.0,
                "taxa_erro": self.erros / total if total else 0.0,
                "substituicoes": self.substituicoes, "tempo_economizado_s": self.tempo_economizado_s,
                "calculos_duplicados": self.calculos_duplicados, "coalescidos": self.coalescidos,
                "respostas_com_erro_nao_cacheadas": self.nao_cacheados_por_erro,
            }


# --------------------------------------------------------------------------------------
# Pipeline concorrente (C3/C4)
# --------------------------------------------------------------------------------------


def gerar_consultas(cenario: str, n: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    todas = [f"{a} {t} em sistemas operacionais?" for t in TOPICOS for a in ASPECTOS]
    if cenario == "repetidas":
        pool = rng.sample(todas, 4)
        return [rng.choice(pool) for _ in range(n)]
    if cenario == "distintas":
        base = todas[:]
        rng.shuffle(base)
        return (base * (n // len(base) + 1))[:n]
    pool = rng.sample(todas, 4)  # misto: metade repetidas, metade distintas
    restantes = [q for q in todas if q not in pool]
    rng.shuffle(restantes)
    n_rep = n // 2
    lista = [rng.choice(pool) for _ in range(n_rep)] + restantes[: n - n_rep]
    rng.shuffle(lista)
    return lista


def instantanea_threads() -> dict[str, Any]:
    try:
        n_kernel = len(os.listdir("/proc/self/task"))
    except OSError:
        n_kernel = None
    return {
        "threads_python": [{"nome": t.name, "native_id": t.native_id, "daemon": t.daemon} for t in threading.enumerate()],
        "threads_kernel_proc_self_task": n_kernel,
    }


def executar_run(cfg: dict[str, Any], base: BaseConhecimento, gerador: Gerador, logger: LogConcorrente,
                 consultas: list[str]) -> dict[str, Any]:
    n_workers, n_prod = cfg["workers"], cfg.get("produtores", 2)
    cache = CacheCompartilhado(cfg["cache_cap"], cfg["politica"], cfg["single_flight"])
    fila: queue.Queue = queue.Queue(maxsize=cfg["fila_cap"])
    resultados: list[dict[str, Any]] = []
    res_lock = threading.Lock()
    amostras_fila: list[int] = []
    ocupado = [0.0] * n_workers
    bloqueio = [0.0] * n_prod
    ordem = {"n": 0}
    snapshot: dict[str, Any] = {}

    def amostrar_fila() -> None:
        with res_lock:
            amostras_fila.append(fila.qsize())

    def worker(idx: int) -> None:
        while True:
            item = fila.get()
            amostrar_fila()
            if item is None:
                fila.task_done()
                return
            t0 = time.perf_counter()
            with res_lock:
                ordem["n"] += 1
                ordem_inicio = ordem["n"]
            consulta = item["consulta"]
            registro: dict[str, Any] = {"consulta": consulta, "produtor": item["produtor"], "seq": item["seq"],
                                        "ordem_inicio": ordem_inicio, "worker": idx}
            try:
                def calcular(consulta: str = consulta) -> tuple[dict[str, Any], float]:
                    rec = base.recuperar(consulta)
                    logger.evento("recuperacao", consulta=consulta, trechos=rec["n_trechos"],
                                  bytes=rec["bytes_contexto_utf8"], tempo_s=round(rec["tempo_recuperacao_s"], 6))
                    ger = gerador.gerar(consulta, rec["contexto"])
                    logger.evento("geracao", consulta=consulta, modo=ger["modo_resposta"],
                                  tentativas=ger["tentativas"], tempo_s=round(ger["tempo_geracao_s"], 6), erro=ger["erro"])
                    resp = {"resposta": ger["resposta"], "modo_resposta": ger["modo_resposta"], "erro": ger["erro"],
                            "tentativas": ger["tentativas"], "tempo_recuperacao_s": rec["tempo_recuperacao_s"],
                            "tempo_geracao_s": ger["tempo_geracao_s"], "bytes_contexto_utf8": rec["bytes_contexto_utf8"],
                            "n_trechos": rec["n_trechos"]}
                    return resp, rec["tempo_recuperacao_s"] + ger["tempo_geracao_s"]

                resp, hit, _custo = cache.obter_ou_calcular(consulta, calcular)
                fim = time.perf_counter()
                enf = item["enfileirado_em"] or t0
                registro.update({
                    "cache_hit": hit, "espera_fila_s": max(0.0, t0 - enf), "latencia_total_s": fim - t0,
                    "latencia_fim_a_fim_s": fim - enf,
                    "tempo_recuperacao_s": 0.0 if hit else resp["tempo_recuperacao_s"],
                    "tempo_geracao_s": 0.0 if hit else resp["tempo_geracao_s"],
                    "bytes_contexto_utf8": 0 if hit else resp["bytes_contexto_utf8"],
                    "n_trechos": 0 if hit else resp["n_trechos"],
                    "modo_resposta": resp["modo_resposta"], "erro": resp["erro"], "tentativas": resp["tentativas"],
                    "resposta_resumo": resp["resposta"][:120],
                })
                logger.evento("cache_hit" if hit else "cache_miss", consulta=consulta, latencia_s=round(fim - t0, 6))
            except Exception as exc:
                registro.update({"erro": f"{type(exc).__name__}: {exc}", "modo_resposta": "falha_worker"})
                logger.evento("falha_worker", consulta=consulta, erro=registro["erro"])
            finally:
                ocupado[idx] += time.perf_counter() - t0
                with res_lock:
                    resultados.append(registro)
                fila.task_done()

    def produtor(idx: int, lista: list[tuple[int, str]]) -> None:
        for seq, consulta in lista:
            item: dict[str, Any] = {"consulta": consulta, "produtor": idx, "seq": seq, "enfileirado_em": None}
            t0 = time.perf_counter()
            fila.put(item)  # bloqueia se a fila estiver cheia (contrapressão)
            item["enfileirado_em"] = time.perf_counter()
            bloqueio[idx] += item["enfileirado_em"] - t0
            amostrar_fila()

    por_produtor = [[(s, c) for s, c in enumerate(consultas) if s % n_prod == p] for p in range(n_prod)]
    logger.evento("run_inicio", **cfg)
    io0 = ler_io_proc()
    with AmostradorRecursos() as amo:
        inicio = time.perf_counter()
        workers = [threading.Thread(target=worker, args=(i,), name=f"worker-{i}") for i in range(n_workers)]
        produtores = [threading.Thread(target=produtor, args=(i, por_produtor[i]), name=f"produtor-{i}") for i in range(n_prod)]
        for t in workers + produtores:
            t.start()
        for t in produtores:
            t.join()
        snapshot = instantanea_threads()
        for _ in workers:
            fila.put(None)  # sentinelas: encerram os consumidores depois de esvaziada a fila
        for t in workers:
            t.join()
        duracao = time.perf_counter() - inicio
    logger.evento("run_fim", workers=n_workers, duracao_s=round(duracao, 4))
    io = diferenca(io0, ler_io_proc())
    io["syscalls_por_s"] = (io.get("syscr", 0) + io.get("syscw", 0)) / duracao if duracao > 0 else None

    validos = [r for r in resultados if "latencia_total_s" in r]
    esperado = collections.Counter(consultas)
    obtido = collections.Counter(r["consulta"] for r in resultados)
    perdidos = sum((esperado - obtido).values())
    duplicados = sum((obtido - esperado).values())
    reorden = 0
    for p in range(n_prod):
        seq_ordem = [r["seq"] for r in sorted((r for r in resultados if r["produtor"] == p), key=lambda r: r["ordem_inicio"])]
        reorden += sum(1 for a, b in zip(seq_ordem, seq_ordem[1:]) if b < a)
    modos: dict[str, int] = {}
    for r in resultados:
        modos[r.get("modo_resposta", "desconhecido")] = modos.get(r.get("modo_resposta", "desconhecido"), 0) + 1
    return {
        "config": cfg, "tempo_total_s": duracao,
        "vazao_consultas_s": len(resultados) / duracao if duracao > 0 else None,
        "consultas_enviadas": len(consultas), "consultas_processadas": len(resultados),
        "itens_perdidos": perdidos, "itens_duplicados": duplicados, "reordenacoes_por_produtor": reorden,
        "erros": sum(1 for r in resultados if r.get("erro")),
        "modos_resposta": modos,
        "latencia": resumo_tempos([r["latencia_total_s"] for r in validos]),
        "latencia_miss": resumo_tempos([r["latencia_total_s"] for r in validos if not r["cache_hit"]]),
        "latencia_hit": resumo_tempos([r["latencia_total_s"] for r in validos if r["cache_hit"]]),
        "recuperacao_miss": resumo_tempos([r["tempo_recuperacao_s"] for r in validos if not r["cache_hit"]]),
        "geracao_miss": resumo_tempos([r["tempo_geracao_s"] for r in validos if not r["cache_hit"]]),
        "io": io,
        "latencia_fim_a_fim": resumo_tempos([r["latencia_fim_a_fim_s"] for r in validos]),
        "espera_fila": resumo_tempos([r["espera_fila_s"] for r in validos]),
        "fila_capacidade": cfg["fila_cap"],
        "fila_tamanho_medio": statistics.mean(amostras_fila) if amostras_fila else None,
        "fila_tamanho_maximo": max(amostras_fila) if amostras_fila else None,
        "bloqueio_produtores_s": sum(bloqueio),
        "utilizacao_workers": [o / duracao if duracao > 0 else None for o in ocupado],
        "utilizacao_workers_media": statistics.mean(o / duracao for o in ocupado) if duracao > 0 else None,
        "metricas_cache": cache.metricas(),
        "recursos": amo.resumo(),
        "threads_observadas": snapshot,
        "resultados_brutos": resultados,
    }


def resumir_grade(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrega as repetições de cada configuração (média e desvio padrão populacional)."""
    grupos: dict[str, list[dict[str, Any]]] = {}
    for r in runs:
        grupos.setdefault(json.dumps(r["config"], sort_keys=True), []).append(r)

    def agg(rs: list[dict[str, Any]], f: Callable[[dict[str, Any]], Any]) -> dict[str, float | None]:
        vals = [f(r) for r in rs]
        vals = [v for v in vals if v is not None]
        if not vals:
            return {"media": None, "desvio": None}
        return {"media": statistics.mean(vals), "desvio": statistics.pstdev(vals) if len(vals) > 1 else 0.0}

    resumo = []
    for chave, rs in grupos.items():
        resumo.append({
            "config": json.loads(chave), "n_repeticoes": len(rs),
            "tempo_total_s": agg(rs, lambda r: r["tempo_total_s"]),
            "vazao_consultas_s": agg(rs, lambda r: r["vazao_consultas_s"]),
            "latencia_media_s": agg(rs, lambda r: r["latencia"]["media_s"]),
            "latencia_mediana_s": agg(rs, lambda r: r["latencia"]["mediana_s"]),
            "latencia_p95_s": agg(rs, lambda r: r["latencia"]["p95_s"]),
            "latencia_fim_a_fim_p95_s": agg(rs, lambda r: r["latencia_fim_a_fim"]["p95_s"]),
            "recuperacao_media_s": agg(rs, lambda r: r["recuperacao_miss"]["media_s"]),
            "geracao_media_s": agg(rs, lambda r: r["geracao_miss"]["media_s"]),
            "geracao_p95_s": agg(rs, lambda r: r["geracao_miss"]["p95_s"]),
            "io_syscalls_por_s": agg(rs, lambda r: r["io"].get("syscalls_por_s")),
            "espera_fila_media_s": agg(rs, lambda r: r["espera_fila"]["media_s"]),
            "fila_tamanho_medio": agg(rs, lambda r: r["fila_tamanho_medio"]),
            "fila_tamanho_maximo": agg(rs, lambda r: r["fila_tamanho_maximo"]),
            "bloqueio_produtores_s": agg(rs, lambda r: r["bloqueio_produtores_s"]),
            "utilizacao_workers_media": agg(rs, lambda r: r["utilizacao_workers_media"]),
            "taxa_acerto": agg(rs, lambda r: r["metricas_cache"]["taxa_acerto"]),
            "tempo_economizado_s": agg(rs, lambda r: r["metricas_cache"]["tempo_economizado_s"]),
            "substituicoes": agg(rs, lambda r: r["metricas_cache"]["substituicoes"]),
            "calculos_duplicados": agg(rs, lambda r: r["metricas_cache"]["calculos_duplicados"]),
            "rss_pico_kb": agg(rs, lambda r: r["recursos"]["rss_pico_kb"]),
            "itens_perdidos": agg(rs, lambda r: r["itens_perdidos"]),
            "itens_duplicados": agg(rs, lambda r: r["itens_duplicados"]),
            "erros": agg(rs, lambda r: r["erros"]),
        })
    return resumo


def construir_grades(args: argparse.Namespace) -> dict[str, list[dict[str, Any]]]:
    maior = max(args.corpus_sizes)
    cache_max = max(args.cache_sizes)
    w_max = max(args.workers)
    base = {"produtores": 2, "politica": "lru", "single_flight": True, "fila_cap": 8, "cenario": "misto", "mmap_indice": False}
    grades: dict[str, list[dict[str, Any]]] = {
        # C3: workers x corpus x cache (3 fatores), cenário misto
        "g1": [dict(base, workers=w, corpus=n, cache_cap=c)
               for n in args.corpus_sizes for c in args.cache_sizes for w in args.workers],
        # C4: repetidas x distintas x misto, sem cache x cache, FIFO x LRU
        # (com consultas distintas a política de remoção é irrelevante: só LRU, economiza chamadas ao LLM)
        "g2": [dict(base, workers=2, corpus=maior, cache_cap=c, politica=p, cenario=cen)
               for cen in ("repetidas", "distintas", "misto") for c in [0] + args.cache_sizes
               for p in (["lru"] if c == 0 or cen == "distintas" else ["fifo", "lru"])],
        # capacidade da fila (contrapressão)
        "g3": [dict(base, workers=w, corpus=maior, cache_cap=cache_max, fila_cap=fc)
               for fc in args.fila_capacidades for w in sorted({1, w_max})],
        # mmap no índice (cache desligado e consultas distintas para forçar recuperação)
        "g4": [dict(base, workers=2, corpus=maior, cache_cap=0, cenario="distintas", mmap_indice=m) for m in (False, True)],
        # single-flight: evita cálculo duplicado quando várias threads pedem a mesma chave
        "g5": [dict(base, workers=w_max, corpus=maior, cache_cap=cache_max, cenario="repetidas", single_flight=sf)
               for sf in (True, False)],
    }
    return grades


# --------------------------------------------------------------------------------------
# C1/C2 — leitura convencional x mmap (fria/quente, sequencial/aleatória)
# --------------------------------------------------------------------------------------


def _criar_arquivo_binario(caminho: Path, mb: int, seed: int = 7) -> None:
    bloco = np.random.default_rng(seed).integers(0, 256, size=MiB, dtype=np.uint8).tobytes()
    with open(caminho, "wb") as arquivo:
        for _ in range(mb):
            arquivo.write(bloco)
        arquivo.flush()
        os.fsync(arquivo.fileno())


def _esvaziar_cache_paginas(caminho: Path) -> bool:
    """Tenta descartar o cache de páginas do arquivo (leitura 'fria'). Sem root, só afeta páginas limpas."""
    try:
        fd = os.open(caminho, os.O_RDONLY)
        try:
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
        return True
    except (AttributeError, OSError):
        return False


def sistema_arquivos(caminho: Path) -> dict[str, str]:
    """Tipo de sistema de arquivos onde `caminho` está (tmpfs = RAM, sem E/S de disco real)."""
    alvo = str(caminho.resolve())
    melhor = ("", "desconhecido", "")
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                partes = linha.split()
                if len(partes) < 3:
                    continue
                dev, ponto, tipo = partes[0], partes[1].replace("\\040", " "), partes[2]
                if (alvo == ponto or alvo.startswith(ponto.rstrip("/") + "/")) and len(ponto) > len(melhor[0]):
                    melhor = (ponto, tipo, dev)
    except OSError:
        pass
    return {"ponto_montagem": melhor[0], "tipo": melhor[1], "dispositivo": melhor[2]}


def _medir_leitura(caminho: Path, modo: str, padrao: str, offsets: list[int]) -> dict[str, Any]:
    tamanho = os.path.getsize(caminho)
    mem0, falt0, io0 = ler_status_proc(), faltas_pagina(), ler_io_proc()
    checksum, bytes_lidos = 0, 0
    with AmostradorRecursos(0.005) as amo:
        t0 = time.perf_counter()
        arquivo = open(caminho, "rb", buffering=0)
        try:
            if modo == "convencional":
                t_aberto = time.perf_counter() - t0
                if padrao == "sequencial":
                    dados = arquivo.read()
                    bytes_lidos = len(dados)
                    checksum = int(np.frombuffer(dados, dtype=np.uint8).sum(dtype=np.uint64))
                    del dados
                else:
                    fd = arquivo.fileno()
                    for off in offsets:
                        bloco = os.pread(fd, 4096, off)
                        checksum ^= bloco[0]
                        bytes_lidos += len(bloco)
            else:
                mapeado = mmap.mmap(arquivo.fileno(), 0, access=mmap.ACCESS_READ)
                t_aberto = time.perf_counter() - t0
                try:
                    with contextlib.suppress(AttributeError, OSError):
                        mapeado.madvise(mmap.MADV_SEQUENTIAL if padrao == "sequencial" else mmap.MADV_RANDOM)
                    if padrao == "sequencial":
                        vetor = np.frombuffer(mapeado, dtype=np.uint8)
                        checksum = int(vetor.sum(dtype=np.uint64))
                        bytes_lidos = tamanho
                        del vetor
                    else:
                        for off in offsets:
                            bloco = mapeado[off:off + 4096]
                            checksum ^= bloco[0]
                            bytes_lidos += len(bloco)
                finally:
                    mapeado.close()
        finally:
            arquivo.close()
        total = time.perf_counter() - t0
    mem1, falt1, io1 = ler_status_proc(), faltas_pagina(), ler_io_proc()
    r = amo.resumo()
    return {
        "tempo_abertura_s": t_aberto, "tempo_leitura_s": total - t_aberto, "tempo_total_s": total,
        "bytes_lidos": bytes_lidos, "checksum": checksum,
        "rss_antes_kb": mem0["rss_kb"], "rss_depois_kb": mem1["rss_kb"], "rss_pico_amostrado_kb": r["rss_pico_kb"],
        "rss_pico_delta_kb": (r["rss_pico_kb"] - mem0["rss_kb"]) if r["rss_pico_kb"] and mem0["rss_kb"] else None,
        "vsz_pico_kb": r["vsz_pico_kb"],
        "mem_available_min_kb": r["mem_available_min_kb"], "cached_max_kb": r["cached_max_kb"],
        "minor_faults_delta": falt1["minor_faults"] - falt0["minor_faults"] if falt1["minor_faults"] is not None else None,
        "major_faults_delta": falt1["major_faults"] - falt0["major_faults"] if falt1["major_faults"] is not None else None,
        "io_read_bytes_delta": io1.get("read_bytes", 0) - io0.get("read_bytes", 0) if io1 else None,
        "io_rchar_delta": io1.get("rchar", 0) - io0.get("rchar", 0) if io1 else None,
        "io_syscr_delta": io1.get("syscr", 0) - io0.get("syscr", 0) if io1 else None,
        "io_ops_por_s": (io1.get("syscr", 0) - io0.get("syscr", 0)) / total if io1 and total > 0 else None,
    }


def exp_leitura(pasta_tmp: Path, tamanhos_mb: list[int], repeticoes: int, logger: LogConcorrente) -> dict[str, Any]:
    linhas: list[dict[str, Any]] = []
    fadvise_ok: list[bool] = []
    for mb in tamanhos_mb:
        caminho = pasta_tmp / f"arquivo_{mb}MiB.bin"
        _criar_arquivo_binario(caminho, mb)
        paginas = os.path.getsize(caminho) // 4096
        rng = random.Random(mb)
        offsets = [rng.randrange(0, paginas) * 4096 for _ in range(min(20000, max(100, paginas // 8)))]
        for rep in range(1, repeticoes + 1):
            for padrao in ("sequencial", "aleatorio"):
                modos = ["convencional", "mmap"] if rep % 2 else ["mmap", "convencional"]  # alterna a ordem
                for modo in modos:
                    for temperatura in ("fria", "quente"):
                        if temperatura == "fria":
                            fadvise_ok.append(_esvaziar_cache_paginas(caminho))
                        medida = _medir_leitura(caminho, modo, padrao, offsets)
                        medida.update({"tamanho_mb": mb, "modo": modo, "padrao": padrao, "temperatura": temperatura, "repeticao": rep})
                        linhas.append(medida)
                        logger.evento("leitura", **{k: medida[k] for k in ("tamanho_mb", "modo", "padrao", "temperatura", "tempo_total_s")})
        caminho.unlink(missing_ok=True)

    grupos: dict[tuple, list[dict[str, Any]]] = {}
    for l in linhas:
        grupos.setdefault((l["tamanho_mb"], l["modo"], l["padrao"], l["temperatura"]), []).append(l)

    def media(rs: list[dict[str, Any]], campo: str) -> float | None:
        v = [r[campo] for r in rs if r[campo] is not None]
        return statistics.mean(v) if v else None

    resumo = [{
        "tamanho_mb": k[0], "modo": k[1], "padrao": k[2], "temperatura": k[3], "n": len(rs),
        "tempo_total": resumo_tempos([r["tempo_total_s"] for r in rs]),
        "tempo_abertura_medio_s": media(rs, "tempo_abertura_s"),
        "rss_pico_delta_medio_kb": media(rs, "rss_pico_delta_kb"),
        "minor_faults_medio": media(rs, "minor_faults_delta"),
        "major_faults_medio": media(rs, "major_faults_delta"),
        "io_read_bytes_medio": media(rs, "io_read_bytes_delta"),
        "io_syscr_medio": media(rs, "io_syscr_delta"),
        "io_ops_por_s_medio": media(rs, "io_ops_por_s"),
    } for k, rs in sorted(grupos.items())]
    seq_frias = [l for l in linhas if l["padrao"] == "sequencial" and l["temperatura"] == "fria"]
    efetivas = sum(1 for l in seq_frias if (l["io_read_bytes_delta"] or 0) >= 0.5 * l["bytes_lidos"])
    return {
        "repeticoes": repeticoes, "tamanhos_mb": tamanhos_mb, "resumo": resumo, "resultados_brutos": linhas,
        "sistema_arquivos_teste": sistema_arquivos(pasta_tmp),
        "frias_efetivas_sequenciais": f"{efetivas}/{len(seq_frias)} (leitura fria só vale se read_bytes >= 50% do arquivo)",
        "fadvise_dontneed_aplicado_em": f"{sum(fadvise_ok)}/{len(fadvise_ok)} execuções frias (a chamada ter sucesso NÃO garante cache descartado)",
        "limitacoes": [
            "Leitura 'fria' usa posix_fadvise(DONTNEED), que só descarta páginas limpas e pode não funcionar em alguns sistemas de arquivos/contêineres; confira major_faults e io_read_bytes.",
            "RSS pico é amostrado a cada 5 ms por uma thread; picos mais curtos podem escapar.",
            "Em Python o custo de interpretação aparece nos padrões aleatórios; compare tendências, não valores absolutos.",
        ],
    }


# --------------------------------------------------------------------------------------
# C5 — pressão de memória (com limites seguros)
# --------------------------------------------------------------------------------------


def _limite_cgroup_restante_kb() -> int | None:
    for arq_max, arq_atual, fator in (("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current", 1024),
                                      ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes", 1024)):
        try:
            texto = Path(arq_max).read_text().strip()
            if texto == "max":
                continue
            maximo, atual = int(texto), int(Path(arq_atual).read_text().strip())
            if maximo < (1 << 60):
                return (maximo - atual) // fator
        except (OSError, ValueError):
            continue
    return None


def limite_seguro_mb(teto_mb: int) -> dict[str, Any]:
    mi = ler_meminfo()
    candidatos = {"teto_configurado": teto_mb * 1024}
    if "MemAvailable" in mi:
        reserva = max(512 * 1024, int(0.25 * mi.get("MemTotal", 0)))
        candidatos["memavailable_menos_reserva"] = mi["MemAvailable"] - reserva
    cg = _limite_cgroup_restante_kb()
    if cg is not None:
        candidatos["cgroup_restante_menos_256MiB"] = cg - 256 * 1024
    fator = min(candidatos, key=lambda k: candidatos[k])
    return {"limite_mb": max(0, candidatos[fator] // 1024), "fator_limitante": fator,
            "candidatos_kb": candidatos, "reserva_memavailable_mb": max(512, int(0.25 * mi.get("MemTotal", 0)) // 1024)}


def exp_pressao_memoria(teto_mb: int, passo_mb: int, n_chunks_lista: list[int], dim: int, lotes: list[int],
                        logger: LogConcorrente) -> dict[str, Any]:
    seguro = limite_seguro_mb(teto_mb)
    logger.evento("c5_inicio", **{k: seguro[k] for k in ("limite_mb", "fator_limitante")})
    saida: dict[str, Any] = {"limite_seguro": seguro}

    # C5a — alocação progressiva tocando todas as páginas, com nova varredura (revela swap-in)
    passos, blocos = [], []
    parou_por = "limite_atingido"
    try:
        alocado = 0
        falt_ini = faltas_pagina()
        while alocado + passo_mb <= seguro["limite_mb"]:
            mi = ler_meminfo()
            reserva_kb = seguro["reserva_memavailable_mb"] * 1024
            if mi.get("MemAvailable") is not None and mi["MemAvailable"] - passo_mb * 1024 < reserva_kb:
                parou_por = "reserva_de_MemAvailable"
                break
            f0, t0 = faltas_pagina(), time.perf_counter()
            blocos.append(np.ones(passo_mb * MiB, dtype=np.uint8))  # escreve em todas as páginas
            alocado += passo_mb
            dt = time.perf_counter() - t0
            f1, st, mi = faltas_pagina(), ler_status_proc(), ler_meminfo()
            passos.append({
                "alocado_mb": alocado, "tempo_passo_s": dt, "mb_por_s": passo_mb / dt if dt else None,
                "rss_kb": st["rss_kb"], "vsz_kb": st["vsz_kb"], "mem_available_kb": mi.get("MemAvailable"),
                "swap_usado_kb": (mi["SwapTotal"] - mi["SwapFree"]) if "SwapTotal" in mi else None,
                "minor_faults_delta": f1["minor_faults"] - f0["minor_faults"] if f1["minor_faults"] is not None else None,
                "major_faults_delta": f1["major_faults"] - f0["major_faults"] if f1["major_faults"] is not None else None,
            })
        # segunda varredura: se houve swap, aparecem faltas maiores e tempo maior
        f0, t0 = faltas_pagina(), time.perf_counter()
        soma = sum(int(b[::4096].sum(dtype=np.uint64)) for b in blocos)
        dt2, f1 = time.perf_counter() - t0, faltas_pagina()
        saida["C5a_alocacao_progressiva"] = {
            "passo_mb": passo_mb, "alocado_total_mb": alocado, "parou_por": parou_por, "passos": passos,
            "segunda_varredura": {"tempo_s": dt2, "checksum": soma,
                                  "minor_faults_delta": f1["minor_faults"] - f0["minor_faults"] if f1["minor_faults"] is not None else None,
                                  "major_faults_delta": f1["major_faults"] - f0["major_faults"] if f1["major_faults"] is not None else None},
            "faltas_totais": diferenca(falt_ini, faltas_pagina()),
            "swap_usado_max_kb": max((p["swap_usado_kb"] or 0) for p in passos) if passos else None,
            "observacao": "O teste NUNCA ultrapassa o limite seguro; portanto pode não provocar swap. Ausência de swap é resultado legítimo, não falha do experimento.",
        }
    finally:
        blocos.clear()

    # C5b — escala do índice: cresce o número de fragmentos e o lote de consultas
    rng = np.random.default_rng(0)
    escalas = []
    for n in n_chunks_lista:
        tam_mb = n * dim * 4 / MiB
        mi = ler_meminfo()
        if tam_mb * 1.5 > seguro["limite_mb"]:
            escalas.append({"n_chunks": n, "tamanho_matriz_mb": tam_mb, "ignorado": "excede o limite seguro (aumente --memory-max-mb se for seguro)"})
            continue
        t0 = time.perf_counter()
        M = rng.standard_normal((n, dim), dtype=np.float32)
        M /= np.linalg.norm(M, axis=1, keepdims=True)
        t_criacao = time.perf_counter() - t0
        item: dict[str, Any] = {"n_chunks": n, "dim": dim, "tamanho_matriz_mb": tam_mb, "tempo_criacao_s": t_criacao,
                                "rss_kb": ler_status_proc()["rss_kb"], "mem_available_kb": ler_meminfo().get("MemAvailable"), "lotes": []}
        for lote in lotes:
            Q = rng.standard_normal((lote, dim), dtype=np.float32)
            Q /= np.linalg.norm(Q, axis=1, keepdims=True)
            tempos = []
            f0 = faltas_pagina()
            for _ in range(10):
                t1 = time.perf_counter()
                S = M @ Q.T
                np.argpartition(-S, 2, axis=0)[:3]
                tempos.append(time.perf_counter() - t1)
            f1 = faltas_pagina()
            item["lotes"].append({"lote": lote, "tempo_lote": resumo_tempos(tempos),
                                  "tempo_por_consulta_medio_s": statistics.mean(tempos) / lote,
                                  "minor_faults_delta": f1["minor_faults"] - f0["minor_faults"] if f1["minor_faults"] is not None else None,
                                  "major_faults_delta": f1["major_faults"] - f0["major_faults"] if f1["major_faults"] is not None else None})
        item["rss_pico_proc_kb"] = ler_status_proc()["rss_pico_proc_kb"]
        escalas.append(item)
        del M
    saida["C5b_escala_indice"] = escalas
    return saida


# --------------------------------------------------------------------------------------
# C6 — falhas controladas com recuperação
# --------------------------------------------------------------------------------------


def exp_falhas(pasta: Path, embedder: Any, args: argparse.Namespace, logger: LogConcorrente) -> dict[str, Any]:
    res: dict[str, Any] = {}
    pasta.mkdir(parents=True, exist_ok=True)
    corpus_ok = gerar_corpus(pasta / "corpus_recuperacao.jsonl", 30)

    # 1) arquivo ausente -> detecta, registra e se recupera com corpus de reserva
    base = BaseConhecimento(embedder, logger)
    try:
        base.ingerir(pasta / "arquivo_inexistente.jsonl", pasta / "idx_ausente")
        res["arquivo_ausente"] = {"detectado": False}
    except FileNotFoundError as exc:
        logger.evento("falha_arquivo_ausente", erro=str(exc))
        info = base.ingerir(corpus_ok, pasta / "idx_recuperacao")
        res["arquivo_ausente"] = {"detectado": True, "tipo": type(exc).__name__, "recuperado": True,
                                  "recuperacao": "ingestão do corpus de reserva", "chunks_apos_recuperacao": info["chunks"]}

    # 2) índice indisponível -> reconstrói o índice a partir do corpus
    base2 = BaseConhecimento(embedder, logger)
    try:
        base2.carregar(pasta / "indice_que_nao_existe", usar_mmap=False)
        res["indice_indisponivel"] = {"detectado": False}
    except FileNotFoundError as exc:
        logger.evento("falha_indice_indisponivel", erro=str(exc))
        base2.ingerir(corpus_ok, pasta / "indice_reconstruido")
        r = base2.recuperar("O que é paginação?")
        res["indice_indisponivel"] = {"detectado": True, "tipo": type(exc).__name__, "recuperado": True,
                                      "recuperacao": "reconstrução do índice", "consulta_pos_recuperacao_ok": r["n_trechos"] > 0}

    # 3) fila saturada: sem retentativa (descarta) x com retentativa/backoff (entrega tudo)
    def teste_fila(com_retentativa: bool) -> dict[str, Any]:
        fila: queue.Queue = queue.Queue(maxsize=4)
        processados = []
        parar = threading.Event()

        def consumidor() -> None:
            while not (parar.is_set() and fila.empty()):
                try:
                    item = fila.get(timeout=0.05)
                except queue.Empty:
                    continue
                time.sleep(0.01)  # consumidor lento
                processados.append(item)
                fila.task_done()

        th = threading.Thread(target=consumidor, name="consumidor-lento")
        th.start()
        rejeicoes = retentativas = descartados = 0
        for i in range(40):
            tentativas = 0
            while True:
                try:
                    fila.put_nowait(i)
                    break
                except queue.Full:
                    rejeicoes += 1
                    if not com_retentativa:
                        descartados += 1
                        break
                    tentativas += 1
                    retentativas += 1
                    time.sleep(min(0.002 * (2 ** min(tentativas, 5)), 0.05))
        parar.set()
        th.join()
        return {"enviados": 40, "processados": len(processados), "rejeicoes_fila_cheia": rejeicoes,
                "retentativas": retentativas, "descartados": descartados}

    res["fila_saturada"] = {"sem_retentativa": teste_fila(False), "com_retentativa_backoff": teste_fila(True),
                            "detectado": True, "tipo": "queue.Full"}
    logger.evento("falha_fila_saturada", **res["fila_saturada"]["sem_retentativa"])

    # 4) falha do serviço externo (LLM): falha transitória (recupera) x permanente (não cacheia erro)
    base2.top_k = 2

    def pipeline(falha_ate: int | None, permanente: bool) -> dict[str, Any]:
        injetor = (lambda c, t: True) if permanente else (lambda c, t: t <= (falha_ate or 0))
        ger = Gerador(args.model, False, args.sim_gen_ms, args.llm_timeout, args.num_predict, args.max_tentativas,
                      0.01, logger, injetor_falha=injetor)
        cache = CacheCompartilhado(10, "lru", True)
        resultados = []
        for consulta in gerar_consultas("distintas", 6, 1):
            def calcular(c: str = consulta) -> tuple[dict[str, Any], float]:
                rec = base2.recuperar(c)
                g = ger.gerar(c, rec["contexto"])
                return g, rec["tempo_recuperacao_s"] + g["tempo_geracao_s"]
            resp, _hit, _ = cache.obter_ou_calcular(consulta, calcular)
            resultados.append({"modo": resp["modo_resposta"], "tentativas": resp["tentativas"], "erro": resp["erro"]})
        return {"consultas": len(resultados), "sucessos": sum(1 for r in resultados if not r["erro"]),
                "falhas_finais": sum(1 for r in resultados if r["erro"]),
                "tentativas_medias": statistics.mean(r["tentativas"] for r in resultados),
                "itens_no_cache_apos_execucao": cache.metricas()["itens_atuais"],
                "erros_nao_cacheados": cache.metricas()["respostas_com_erro_nao_cacheadas"],
                "detalhe": resultados}

    res["falha_servico_externo"] = {
        "tipo_injecao": "ConnectionError injetado no gerador (simulação do Ollama indisponível)",
        "transitoria_2_primeiras_tentativas": pipeline(2, False),
        "permanente": pipeline(None, True),
    }
    if ollama is not None and not args.skip_llm:
        try:
            ollama.Client(timeout=15).generate(model="modelo-inexistente-genresearch", prompt="teste")
            res["falha_servico_externo"]["ollama_real_modelo_inexistente"] = {"detectado": False}
        except Exception as exc:
            res["falha_servico_externo"]["ollama_real_modelo_inexistente"] = {"detectado": True, "erro": f"{type(exc).__name__}: {exc}"[:300]}
    else:
        res["falha_servico_externo"]["ollama_real_modelo_inexistente"] = "não executado (--skip-llm ou pacote ollama ausente)"

    # 5) limite de memória: processo filho com RLIMIT_AS -> MemoryError controlado
    codigo = ("import resource,sys\nL=256*1024*1024\nresource.setrlimit(resource.RLIMIT_AS,(L,L))\n"
              "try:\n    b=bytearray(512*1024*1024)\n    print('SEM_ERRO')\nexcept MemoryError:\n    print('MemoryError')\n")
    try:
        proc = subprocess.run([sys.executable, "-c", codigo], capture_output=True, text=True, timeout=30)
        res["limite_memoria"] = {"limite_as_mb": 256, "tentativa_alocar_mb": 512, "saida": proc.stdout.strip(),
                                 "detectado": proc.stdout.strip() == "MemoryError", "codigo_saida_processo_filho": proc.returncode,
                                 "recuperacao": "o processo principal não é afetado: o limite vale só no filho"}
    except subprocess.TimeoutExpired:
        res["limite_memoria"] = {"detectado": False, "erro": "timeout"}
    return res


# --------------------------------------------------------------------------------------
# Parte D — sincronização (falhas controladas + correções)
# --------------------------------------------------------------------------------------

JANELA = 0.00005  # janela deliberada de escalonamento para tornar a corrida observável


class CacheDemo:
    """Cache simples para a demonstração: `usar_lock=False` reproduz a condição de corrida."""

    def __init__(self, capacidade: int, usar_lock: bool):
        self.cap, self.d = capacidade, {}
        self.lock = threading.Lock() if usar_lock else contextlib.nullcontext()
        self.acertos = self.erros = self.excecoes = self.maximo = 0

    def acessar(self, chave: int) -> None:
        with self.lock:
            if chave in self.d:
                atual = self.acertos
                time.sleep(JANELA)
                self.acertos = atual + 1  # atualização perdida sem lock
                return
            atual = self.erros
            time.sleep(JANELA)
            self.erros = atual + 1
            if len(self.d) >= self.cap:  # verifica...
                time.sleep(JANELA)
                try:
                    del self.d[next(iter(self.d))]
                except (KeyError, RuntimeError, StopIteration):
                    self.excecoes += 1
            time.sleep(JANELA)
            self.d[chave] = True  # ...e insere (check-then-act)
            self.maximo = max(self.maximo, len(self.d))


def demo_cache(usar_lock: bool, threads: int = 8, ops: int = 150, capacidade: int = 10, universo: int = 40) -> dict[str, Any]:
    cache = CacheDemo(capacidade, usar_lock)

    def tarefa(sem: int) -> None:
        rng = random.Random(sem)
        for _ in range(ops):
            cache.acessar(rng.randrange(universo))

    ts = [threading.Thread(target=tarefa, args=(i,), name=f"demo-cache-{i}") for i in range(threads)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    total = threads * ops
    contados = cache.acertos + cache.erros
    return {"com_lock": usar_lock, "operacoes": total, "capacidade": capacidade, "tamanho_final": len(cache.d),
            "tamanho_maximo_observado": cache.maximo, "violou_capacidade": cache.maximo > capacidade,
            "contagens_registradas": contados, "contagens_perdidas": total - contados,
            "excecoes_de_corrida": cache.excecoes, "tempo_s": time.perf_counter() - t0}


class ContadorInseguro:
    def __init__(self) -> None:
        self.valor = 0

    def incrementar(self) -> None:
        atual = self.valor
        time.sleep(JANELA)
        self.valor = atual + 1


class ContadorSeguro:
    def __init__(self) -> None:
        self.valor, self.lock = 0, threading.Lock()

    def incrementar(self) -> None:
        with self.lock:
            atual = self.valor
            time.sleep(JANELA)
            self.valor = atual + 1


def demo_contador(tipo: str, threads: int = 10, incrementos: int = 50) -> dict[str, Any]:
    c = ContadorInseguro() if tipo == "inseguro" else ContadorSeguro()
    ts = [threading.Thread(target=lambda: [c.incrementar() for _ in range(incrementos)]) for _ in range(threads)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    esperado = threads * incrementos
    return {"tipo": tipo, "threads": threads, "valor_esperado": esperado, "valor_obtido": c.valor,
            "atualizacoes_perdidas": esperado - c.valor, "correto": c.valor == esperado, "tempo_s": time.perf_counter() - t0}


def demo_log_intercalado(com_lock: bool, threads: int = 8, linhas: int = 100) -> dict[str, Any]:
    lock = threading.Lock() if com_lock else contextlib.nullcontext()
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", suffix=".log", delete=False, buffering=1) as arq:
        caminho = arq.name

        def tarefa(i: int) -> None:
            for n in range(linhas):
                with lock:
                    arq.write(f"[T{i} #{n}")  # escrita em duas partes: sem lock, outra thread se intercala
                    time.sleep(JANELA)
                    arq.write("] evento ok\n")

        ts = [threading.Thread(target=tarefa, args=(i,)) for i in range(threads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
    conteudo = Path(caminho).read_text(encoding="utf-8").split("\n")
    Path(caminho).unlink(missing_ok=True)
    linhas_ok = sum(1 for l in conteudo if re.fullmatch(r"\[T\d+ #\d+\] evento ok", l))
    return {"com_lock": com_lock, "linhas_esperadas": threads * linhas, "linhas_validas": linhas_ok,
            "linhas_corrompidas": threads * linhas - linhas_ok}


class BufferLimitado:
    """Produtor/consumidor clássico: semáforos `vazias` (empty), `cheias` (full) e `mutex`."""

    def __init__(self, capacidade: int):
        self.buf: collections.deque = collections.deque()
        self.capacidade = capacidade
        self.vazias = threading.Semaphore(capacidade)
        self.cheias = threading.Semaphore(0)
        self.mutex = threading.Lock()
        self.ocupacao_max = 0

    def produzir(self, item: Any) -> None:
        self.vazias.acquire()
        with self.mutex:
            self.buf.append(item)
            self.ocupacao_max = max(self.ocupacao_max, len(self.buf))
        self.cheias.release()

    def consumir(self, timeout: float | None = None) -> Any:
        if not self.cheias.acquire(timeout=timeout):
            raise queue.Empty
        with self.mutex:
            item = self.buf.popleft()
        self.vazias.release()
        return item


def demo_buffer_limitado(n_prod: int = 3, n_cons: int = 3, itens: int = 200, capacidade: int = 5) -> dict[str, Any]:
    buf = BufferLimitado(capacidade)
    consumidos: list[int] = []
    lock_c = threading.Lock()
    FIM = -1

    def prod(p: int) -> None:
        for i in range(itens):
            buf.produzir(p * 10_000 + i)

    def cons() -> None:
        while True:
            item = buf.consumir()
            if item == FIM:
                return
            with lock_c:
                consumidos.append(item)

    ps = [threading.Thread(target=prod, args=(p,), name=f"prod-{p}") for p in range(n_prod)]
    cs = [threading.Thread(target=cons, name=f"cons-{c}") for c in range(n_cons)]
    t0 = time.perf_counter()
    for t in ps + cs:
        t.start()
    for t in ps:
        t.join()
    for _ in cs:
        buf.produzir(FIM)
    for t in cs:
        t.join()
    esperado = {p * 10_000 + i for p in range(n_prod) for i in range(itens)}
    contagem = collections.Counter(consumidos)
    # consumidor bloqueado: buffer vazio, consumo com timeout (recuperação por timeout)
    vazio = BufferLimitado(2)
    t1 = time.perf_counter()
    try:
        vazio.consumir(timeout=0.3)
        bloqueio = {"bloqueou": False}
    except queue.Empty:
        bloqueio = {"bloqueou": True, "tempo_bloqueado_s": time.perf_counter() - t1, "recuperacao": "timeout de 0,3 s"}
    return {"produtores": n_prod, "consumidores": n_cons, "capacidade": capacidade, "itens_produzidos": len(esperado),
            "itens_consumidos": len(consumidos), "itens_perdidos": len(esperado - set(consumidos)),
            "itens_duplicados": sum(v - 1 for v in contagem.values() if v > 1),
            "ocupacao_maxima": buf.ocupacao_max, "respeitou_capacidade": buf.ocupacao_max <= capacidade,
            "tempo_s": time.perf_counter() - t0, "consumidor_bloqueado": bloqueio}


def demo_deadlock(timeout: float = 0.4) -> dict[str, Any]:
    """Espera circular ISOLADA: duas threads, duas travas em ordens opostas, com timeout e recuperação."""
    a, b = threading.Lock(), threading.Lock()
    barreira = threading.Barrier(2, timeout=5)
    eventos: list[str] = []
    guarda = threading.Lock()

    def registrar(msg: str) -> None:
        with guarda:
            eventos.append(msg)

    def tarefa(nome: str, primeira: threading.Lock, segunda: threading.Lock) -> None:
        primeira.acquire()
        barreira.wait()  # garante que ambas seguram a 1ª trava antes de pedir a 2ª
        if segunda.acquire(timeout=timeout):
            segunda.release()
            registrar(f"{nome}: obteve as duas travas (sem deadlock)")
        else:
            registrar(f"{nome}: TIMEOUT esperando a 2ª trava -> espera circular detectada; libera a 1ª")
        primeira.release()

    t0 = time.perf_counter()
    ts = [threading.Thread(target=tarefa, args=("T1", a, b), name="dl-T1", daemon=True),
          threading.Thread(target=tarefa, args=("T2", b, a), name="dl-T2", daemon=True)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=5)
    detectado = sum("TIMEOUT" in e for e in eventos) >= 1

    def ordenada(nome: str) -> None:  # correção: ordem global (a antes de b)
        with a:
            with b:
                registrar(f"{nome}: ordem global de aquisição -> sucesso")

    ts2 = [threading.Thread(target=ordenada, args=(n,), daemon=True) for n in ("T1", "T2")]
    for t in ts2:
        t.start()
    for t in ts2:
        t.join(timeout=5)
    return {"deadlock_detectado_por_timeout": detectado, "timeout_s": timeout,
            "recuperado_com_ordem_global": all(not t.is_alive() for t in ts2),
            "threads_encerradas": all(not t.is_alive() for t in ts + ts2), "eventos": eventos,
            "tempo_s": time.perf_counter() - t0}


def _filho_deadlock() -> None:
    """Executa em PROCESSO FILHO: deadlock real (duas travas em ordem oposta, sem timeout). Nunca termina sozinho."""
    a, b = threading.Lock(), threading.Lock()
    barreira = threading.Barrier(2)

    def tarefa(primeira: threading.Lock, segunda: threading.Lock) -> None:
        with primeira:
            barreira.wait()
            with segunda:  # espera circular: nunca retorna
                pass

    for par in ((a, b), (b, a)):
        threading.Thread(target=tarefa, args=par, daemon=True).start()
    print("filho: 2 threads em espera circular (deadlock real)", flush=True)
    time.sleep(3600)


def demo_deadlock_isolado(espera_s: float = 2.0) -> dict[str, Any]:
    """Deadlock real em processo isolado, detectado por watchdog e encerrado pelo pai; mais a variante com timeout."""
    caminho = os.path.abspath(__file__)
    resultado: dict[str, Any] = {}
    proc = subprocess.Popen([sys.executable, caminho, "--filho-deadlock"], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    try:
        proc.wait(timeout=espera_s)
        resultado["sem_timeout_processo_isolado"] = {"deadlock_ocorreu": False, "codigo_saida": proc.returncode}
    except subprocess.TimeoutExpired:
        proc.kill()
        saida, _ = proc.communicate()
        resultado["sem_timeout_processo_isolado"] = {
            "deadlock_ocorreu": True, "pid_filho": proc.pid, "saida_filho": saida.strip(),
            "evidencia": f"o filho não terminou em {espera_s}s", "recuperacao": "watchdog do pai: kill() só do processo filho",
            "codigo_saida_filho": proc.returncode}
    resultado["com_timeout_e_ordem_global"] = demo_deadlock()
    return resultado


def exp_sincronizacao(repeticoes: int, logger: LogConcorrente) -> dict[str, Any]:
    saida = {
        "D2_cache_compartilhado_corrida": {
            "sem_lock": [demo_cache(False) for _ in range(repeticoes)],
            "com_lock": [demo_cache(True) for _ in range(repeticoes)],
        },
        "D_contador_atualizacao_perdida": {
            "inseguro": [demo_contador("inseguro") for _ in range(repeticoes)],
            "seguro": [demo_contador("seguro") for _ in range(repeticoes)],
        },
        "D_log_intercalado": {
            "sem_lock": [demo_log_intercalado(False) for _ in range(repeticoes)],
            "com_lock": [demo_log_intercalado(True) for _ in range(repeticoes)],
        },
        "D1_buffer_limitado_semaforos": [demo_buffer_limitado() for _ in range(repeticoes)],
        "D_deadlock_isolado": demo_deadlock_isolado(),
        "nota": "A corrida usa uma janela deliberada (sleep) para ser reproduzível; sem ela, o GIL a torna rara, mas não impossível.",
    }
    logger.evento("sincronizacao_fim")
    return saida


# --------------------------------------------------------------------------------------
# Programa principal
# --------------------------------------------------------------------------------------


PERFIL_RAPIDO: dict[str, Any] = {
    "repetitions": 3, "corpus_sizes": [60, 200], "cache_sizes": [10], "workers": [1, 2, 4],
    "fila_capacidades": [1, 8], "consultas": 10, "read_sizes_mb": [1, 128], "num_predict": 24,
    "llm_timeout": 45.0, "max_tentativas": 2, "memory_max_mb": 256, "memory_step_mb": 64,
    "chunks_escala": [10000, 50000], "lotes": [1, 16], "grades": ["g1", "g2"],
}


def simular_misses(cfg: dict[str, Any], consultas: list[str]) -> int:
    """Conta quantas gerações (misses) uma sequência de consultas causaria, sem concorrência."""
    cap = cfg["cache_cap"]
    if cap == 0:
        return len(consultas)
    cache: OrderedDict[str, int] = OrderedDict()
    misses = 0
    for q in consultas:
        if q in cache:
            if cfg["politica"] == "lru":
                cache.move_to_end(q)
        else:
            misses += 1
            if len(cache) >= cap:
                cache.popitem(last=False)
            cache[q] = 1
    return misses


def escolher_consultas(cfgs: list[dict[str, Any]], args: argparse.Namespace, t_gen_s: float,
                       orcamento_s: float, minimo: int = 4) -> tuple[int, float, int]:
    """Maior nº de consultas por execução cuja estimativa (pior caso: LLM sequencial) cabe no orçamento."""
    n_max = max(1, args.consultas)
    n_min = min(minimo, n_max)
    est, misses = 0.0, 0
    for n in range(n_max, n_min - 1, -1):
        misses = sum(simular_misses(cfg, gerar_consultas(cfg["cenario"], n, args.seed + rep))
                     for rep in range(1, args.repetitions + 1) for cfg in cfgs)
        est = misses * t_gen_s * 1.15
        if est <= orcamento_s:
            return n, est, misses
    return n_min, est, misses


def verificar_log(caminho: Path) -> dict[str, Any]:
    """Confere se todas as linhas do log têm o formato esperado (detecta intercalação/corrupção)."""
    padrao = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3} \| [^|]+ \| [a-z_0-9]+ \| \{.*\}$")
    total = invalidas = 0
    with open(caminho, "r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            total += 1
            if not padrao.match(linha.rstrip("\n")):
                invalidas += 1
    return {"linhas": total, "linhas_corrompidas": invalidas}


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "--filho-deadlock":  # uso interno (Parte D)
        _filho_deadlock()
        return 0
    p = argparse.ArgumentParser(description="GenResearch-Servidor — experimentos da Atividade 2 de SO.")
    p.add_argument("--repetitions", type=int, default=3, help="Repetições por configuração (mínimo 3).")
    p.add_argument("--corpus-sizes", type=int, nargs="+", default=[300, 1200], help="Nº de documentos sintéticos.")
    p.add_argument("--cache-sizes", type=int, nargs="+", default=[10, 50], help="Capacidades do cache.")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4], help="Nº de consumidores (threads).")
    p.add_argument("--fila-capacidades", type=int, nargs="+", default=[1, 8, 64], help="Capacidades da fila (g3).")
    p.add_argument("--consultas", type=int, default=48, help="Consultas por execução (reduza com LLM real).")
    p.add_argument("--read-sizes-mb", type=int, nargs="+", default=[1, 16, 128], help="Tamanhos dos arquivos de C1/C2.")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    p.add_argument("--embedding-mode", choices=["auto", "real", "hash"], default="auto")
    p.add_argument("--skip-llm", action="store_true", help="Simula a geração (sleep); NÃO mede inferência real.")
    p.add_argument("--sim-gen-ms", type=float, default=50.0, help="Custo da geração simulada em ms.")
    p.add_argument("--num-predict", type=int, default=96, help="Máx. de tokens gerados pelo Ollama.")
    p.add_argument("--llm-timeout", type=float, default=120.0)
    p.add_argument("--max-tentativas", type=int, default=3)
    p.add_argument("--memory-max-mb", type=int, default=1024, help="Teto do teste C5 (além dos limites seguros automáticos).")
    p.add_argument("--memory-step-mb", type=int, default=64)
    p.add_argument("--chunks-escala", type=int, nargs="+", default=[10000, 50000, 200000, 500000])
    p.add_argument("--lotes", type=int, nargs="+", default=[1, 32], help="Tamanhos de lote de consultas (C5b).")
    p.add_argument("--only", nargs="+", choices=EXPERIMENTOS, default=EXPERIMENTOS)
    p.add_argument("--grades", nargs="+", choices=GRADES, default=GRADES, help="Grades de C3/C4 a executar.")
    p.add_argument("--tmp-dir", default=None,
                   help="Pasta EM DISCO (não tmpfs) p/ arquivos temporários; padrão: <data-dir>/tmp. Em Fedora/Ubuntu o /tmp pode ser RAM.")
    p.add_argument("--output-dir", default="resultados")
    p.add_argument("--data-dir", default="dados")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--perfil", choices=["completo", "rapido"], default="completo",
                   help="'rapido': parâmetros reduzidos + orçamento de ~8,5 min com LLM real.")
    p.add_argument("--budget-min", type=float, default=None,
                   help="Orçamento total (min): calibra o LLM, reduz consultas e pula execuções que não cabem.")
    pre, _ = p.parse_known_args()
    if pre.perfil == "rapido":
        p.set_defaults(**PERFIL_RAPIDO)
    args = p.parse_args()
    if args.perfil == "rapido" and args.budget_min is None:
        args.budget_min = 8.5
    t_programa = time.monotonic()

    if args.repetitions < 3:
        p.error("--repetitions deve ser >= 3 (mínimo do enunciado).")
    if any(n < 1 for n in args.corpus_sizes + args.workers + args.fila_capacidades + args.read_sizes_mb) or any(c < 0 for c in args.cache_sizes):
        p.error("Tamanhos, workers e capacidades de fila devem ser positivos (cache >= 0).")
    if not 1 <= args.memory_max_mb <= 8192:
        p.error("--memory-max-mb deve estar entre 1 e 8192.")

    saida = Path(args.output_dir)
    saida.mkdir(parents=True, exist_ok=True)
    dados = Path(args.data_dir)
    logger = LogConcorrente(saida / "genresearch.log")
    logger.evento("inicio", argumentos=vars(args))
    caminho_json = saida / "relatorio_metricas.json"
    relatorio: dict[str, Any] = {"atividade": "Sistemas Operacionais — Atividade 2 (AV2)", "inicio_execucao": agora(), "parametros": vars(args)}

    def salvar() -> None:
        with open(caminho_json, "w", encoding="utf-8") as arquivo:
            json.dump(relatorio, arquivo, indent=2, ensure_ascii=False, default=str)

    try:
        embedder, aviso_emb = criar_embedder(args.embedding_mode, args.embedding_model)
        relatorio["avisos"] = [
            "RSS pico vem de amostragem periódica (thread); RSS antes/depois não é pico.",
            "Faltas de página (getrusage) são cumulativas do processo; os deltas incluem atividade de outras threads.",
            "O cache de páginas do SO afeta execuções seguintes; veja fria x quente em C1/C2.",
            "Com --skip-llm a geração é SIMULADA (sleep); não representa inferência real.",
            "Com threads, a parte de CPU em Python/NumPy é limitada pelo GIL (parcialmente liberado por NumPy/BLAS); a geração via Ollama é E/S de rede.",
        ]
        if aviso_emb:
            relatorio["avisos"].append(aviso_emb)
            print(f"[AVISO] {aviso_emb}")
        if args.skip_llm:
            print("[AVISO] --skip-llm: geração simulada.")

        if "ambiente" in args.only:
            print("[A] Coletando ambiente...")
            relatorio["A_ambiente"] = coletar_ambiente(args, saida / "ambiente", embedder.modo, embedder.nome)
            salvar()

        tmp_base = Path(args.tmp_dir) if args.tmp_dir else dados / "tmp"
        tmp_base.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="genresearch_", dir=tmp_base) as tmp:
            tmp = Path(tmp)
            if "leitura" in args.only:
                print(f"[C1/C2] Leitura convencional x mmap em {tmp} ...")
                relatorio["C1_C2_leitura"] = exp_leitura(tmp, args.read_sizes_mb, args.repetitions, logger)
                fs = relatorio["C1_C2_leitura"]["sistema_arquivos_teste"]
                print(f"[C1/C2] sistema de arquivos: {fs['tipo']} ({fs['dispositivo']}); frias efetivas: "
                      f"{relatorio['C1_C2_leitura']['frias_efetivas_sequenciais']}")
                if fs["tipo"] in ("tmpfs", "ramfs"):
                    relatorio["avisos"].append("C1/C2 rodou em tmpfs/ramfs (RAM): não há E/S de disco; use --tmp-dir em disco.")
                salvar()

            if "concorrencia" in args.only:
                gerador = Gerador(args.model, not args.skip_llm, args.sim_gen_ms, args.llm_timeout, args.num_predict,
                                  args.max_tentativas, 0.5, logger)
                ingestoes: dict[str, Any] = {}
                for n in sorted(set(args.corpus_sizes)):
                    corpus = gerar_corpus(dados / f"corpus_{n}.jsonl", n, args.seed)
                    reps = []
                    for r in range(args.repetitions):  # repete a ingestão para obter média/mediana/p95
                        reps.append(BaseConhecimento(embedder, logger).ingerir(corpus, dados / "indices" / f"n{n}"))
                    ingestoes[str(n)] = {"repeticoes": reps, "tempo_ingestao_total": resumo_tempos([x["tempo_ingestao_total_s"] for x in reps])}
                relatorio["B_ingestao"] = ingestoes
                salvar()

                bases: dict[tuple[int, bool], BaseConhecimento] = {}

                def obter_base(n: int, usar_mmap: bool) -> BaseConhecimento:
                    if (n, usar_mmap) not in bases:
                        b = BaseConhecimento(embedder, logger)
                        b.carregar(dados / "indices" / f"n{n}", usar_mmap)
                        bases[(n, usar_mmap)] = b
                    return bases[(n, usar_mmap)]

                grades = construir_grades(args)
                orcamento: dict[str, Any] = {"budget_min": args.budget_min}
                if not args.skip_llm:
                    # Aquecimento (carrega o modelo na memória) + calibração; falha cedo se o LLM real não responde.
                    print(f"[LLM] Aquecendo {args.model}...")
                    aq = gerador.gerar("Aquecimento do modelo.", "Contexto curto de teste.")
                    cal = [gerador.gerar(f"Calibração {i}", "Sistemas operacionais gerenciam memória.") for i in range(2)]
                    if any(x["modo_resposta"] != "ollama_real" for x in [aq] + cal):
                        erro = next(x["erro"] for x in [aq] + cal if x["modo_resposta"] != "ollama_real")
                        raise RuntimeError(f"LLM real indisponível ({erro}). Rode 'ollama serve', 'ollama pull {args.model}' "
                                           f"e instale o pacote 'ollama'; ou use --skip-llm (simulado).")
                    t_gen = statistics.mean(x["tempo_geracao_s"] for x in cal)
                    orcamento.update(warmup_s=aq["tempo_geracao_s"], t_geracao_calibrado_s=t_gen)
                    print(f"[LLM] warmup {aq['tempo_geracao_s']:.1f}s; geração calibrada {t_gen:.2f}s/consulta")
                else:
                    t_gen = args.sim_gen_ms / 1000.0
                consultas_n, fim_conc = args.consultas, None
                if args.budget_min:
                    reserva_pos = 60.0 if any(e in args.only for e in ("memoria", "falhas", "sincronizacao")) else 5.0
                    fim_conc = t_programa + args.budget_min * 60 - reserva_pos
                    cfgs = [c for g in args.grades for c in grades[g]]
                    consultas_n, est, misses = escolher_consultas(cfgs, args, t_gen, fim_conc - time.monotonic())
                    orcamento.update(consultas_por_execucao=consultas_n, geracoes_estimadas=misses, estimativa_concorrencia_s=est,
                                     reserva_fases_posteriores_s=reserva_pos)
                    print(f"[orçamento] {consultas_n} consultas/execução, ~{misses} gerações, estimativa {est:.0f}s")
                planejadas = args.repetitions * sum(len(grades[g]) for g in args.grades)
                resultado_grades: dict[str, Any] = {g: {"runs": [], "resumo": []} for g in args.grades}
                relatorio["C3_C4_concorrencia_cache"] = resultado_grades
                relatorio["orcamento_tempo"] = orcamento
                executadas, puladas = 0, []
                t_conc = time.monotonic()
                for rep in range(1, args.repetitions + 1):  # repetição por fora: se faltar tempo, todas as configs têm n-1 reps
                    for nome in args.grades:
                        for cfg in grades[nome]:
                            if fim_conc is not None and time.monotonic() > fim_conc:
                                puladas.append({"grade": nome, "repeticao": rep, "config": cfg})
                                continue
                            print(f"[{nome}] {executadas + 1}/{planejadas} rep={rep} {cfg['cenario']} w={cfg['workers']} "
                                  f"docs={cfg['corpus']} cache={cfg['cache_cap']} {cfg['politica']} "
                                  f"(decorrido {time.monotonic() - t_programa:.0f}s)")
                            consultas = gerar_consultas(cfg["cenario"], consultas_n, args.seed + rep)
                            run = executar_run(cfg, obter_base(cfg["corpus"], cfg["mmap_indice"]), gerador, logger, consultas)
                            run["repeticao"] = rep
                            resultado_grades[nome]["runs"].append(run)
                            executadas += 1
                    salvar()
                for nome in args.grades:
                    resultado_grades[nome]["resumo"] = resumir_grade(resultado_grades[nome]["runs"])
                orcamento.update(execucoes_planejadas=planejadas, execucoes_realizadas=executadas, execucoes_puladas=len(puladas),
                                 puladas=puladas[:60], tempo_concorrencia_s=time.monotonic() - t_conc)
                if puladas:
                    relatorio["avisos"].append(f"ORÇAMENTO DE TEMPO: {len(puladas)} de {planejadas} execuções foram puladas; "
                                               "algumas configurações têm menos de 3 repetições. Declare isso no relatório.")
                    print(f"[AVISO] {len(puladas)} execuções puladas por falta de tempo.")
                relatorio["C3_C4_descricao_grades"] = {
                    "g1": "C3: workers x corpus x capacidade do cache (cenário misto, LRU)",
                    "g2": "C4: cenário (repetidas/distintas/misto) x capacidade (0 = sem cache) x política (FIFO/LRU)",
                    "g3": "capacidade da fila x workers (contrapressão)",
                    "g4": "índice em memória x índice mapeado (np.load mmap_mode='r'), cache desligado",
                    "g5": "single-flight ligado x desligado (cálculo duplicado)",
                    "mecanismo_de_trabalhadores": "threads (carga mista: NumPy/BLAS libera o GIL; geração via Ollama é E/S de rede)",
                }

            if "memoria" in args.only:
                print("[C5] Pressão de memória (limites seguros)...")
                dim = embedder.dim
                relatorio["C5_pressao_memoria"] = exp_pressao_memoria(
                    args.memory_max_mb, args.memory_step_mb, args.chunks_escala, dim, args.lotes, logger)
                salvar()

            if "falhas" in args.only:
                print("[C6] Falhas controladas...")
                relatorio["C6_falhas_controladas"] = exp_falhas(tmp / "falhas", embedder, args, logger)
                salvar()

            if "sincronizacao" in args.only:
                print("[D] Sincronização...")
                relatorio["D_sincronizacao"] = exp_sincronizacao(args.repetitions, logger)
                salvar()
    except Exception as exc:
        relatorio["erro_fatal"] = {"tipo": type(exc).__name__, "mensagem": str(exc), "traceback": traceback.format_exc()}
        logger.evento("erro_fatal", **relatorio["erro_fatal"])
        print(f"[ERRO] {type(exc).__name__}: {exc}", file=sys.stderr)
    finally:
        relatorio["fim_execucao"] = agora()
        relatorio["tempo_total_execucao_s"] = time.monotonic() - t_programa
        print(f"\nTempo total: {relatorio['tempo_total_execucao_s']:.0f}s")
        relatorio["log_linhas"] = logger.linhas
        logger.evento("encerramento")
        logger.fechar()
        with contextlib.suppress(OSError):
            relatorio["log_integridade"] = verificar_log(saida / "genresearch.log")
        salvar()
        print(f"\nRelatório: {caminho_json}\nLog: {saida / 'genresearch.log'}")
    return 1 if "erro_fatal" in relatorio else 0


if __name__ == "__main__":
    raise SystemExit(main())
