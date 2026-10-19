#!/usr/bin/env bash
# Coleta evidências do SO enquanto o experimento roda (Parte A, E e requisito de "evidências de
# processos, threads, chamadas de sistema, memória, armazenamento e sincronização").
#
# Uso:   bash executar_evidencias.sh [argumentos extras para atividade2.py]
# Ex.:   bash executar_evidencias.sh --skip-llm --embedding-mode hash
#
# Gera em resultados/evidencias/:
#   ambiente_*.txt        uname, os-release, lscpu, free, lsblk, df, swapon, meminfo, git
#   vmstat.txt iostat.txt amostras de 1 s durante TODA a execução
#   time_v.txt            /usr/bin/time -v (pico de RSS, faltas de página, trocas de contexto)
#   threads_ps.txt        ps -L (LWPs/threads) amostrado a cada 2 s
#   top_threads.txt       top -H por thread
#   proc_status.txt       /proc/<pid>/status (Threads, VmRSS, VmHWM, ctx switches)
#   strace_resumo.txt     strace -f -c (contagem de chamadas: read, mmap, futex, ...)
#   strace_leitura.txt    strace de openat/read/mmap/munmap/madvise na leitura convencional x mmap
#   strace_sync.txt       strace de futex na demonstração de sincronização
set -u
cd "$(dirname "$0")"
EV=resultados/evidencias
mkdir -p "$EV" resultados
PY=${PYTHON:-python3}

echo "[1/5] Ambiente"
{ uname -a; echo; cat /etc/os-release; } > "$EV/ambiente_os.txt" 2>&1
lscpu > "$EV/ambiente_lscpu.txt" 2>&1
{ free -h; echo; swapon --show; } > "$EV/ambiente_memoria.txt" 2>&1
cat /proc/meminfo > "$EV/ambiente_proc_meminfo.txt" 2>&1
{ lsblk; echo; df -h; echo; findmnt -T . ; } > "$EV/ambiente_armazenamento.txt" 2>&1
{ git rev-parse HEAD; git status --short; } > "$EV/ambiente_git.txt" 2>&1
nvidia-smi > "$EV/ambiente_gpu.txt" 2>&1 || echo "sem GPU NVIDIA / nvidia-smi indisponível" > "$EV/ambiente_gpu.txt"
{ ollama --version; ollama list; } > "$EV/ambiente_ollama.txt" 2>&1 || true

echo "[2/5] Execução completa com monitores (vmstat, iostat, ps -L, top -H)"
vmstat 1 > "$EV/vmstat.txt" 2>&1 & VM=$!
(command -v iostat >/dev/null && iostat -xz 1 > "$EV/iostat.txt" 2>&1) & IO=$!
if [ -x /usr/bin/time ]; then
  /usr/bin/time -v -o "$EV/time_v.txt" $PY atividade2.py "$@" > resultados/stdout.txt 2>&1 &
else
  echo "/usr/bin/time ausente (instale o pacote 'time')" > "$EV/time_v.txt"
  $PY atividade2.py "$@" > resultados/stdout.txt 2>&1 &
fi
SH=$!
sleep 1
PID=$(pgrep -f "atividade2.py" | tail -1)
: > "$EV/threads_ps.txt"; : > "$EV/top_threads.txt"; : > "$EV/proc_status.txt"
while kill -0 "$SH" 2>/dev/null; do
  { date +%T; ps -L -p "$PID" -o pid,tid,psr,pcpu,stat,nlwp,comm 2>/dev/null; echo; } >> "$EV/threads_ps.txt"
  { date +%T; top -H -b -n 1 -p "$PID" 2>/dev/null | head -20; echo; } >> "$EV/top_threads.txt"
  { date +%T; grep -E 'Threads|VmSize|VmRSS|VmHWM|ctxt' "/proc/$PID/status" 2>/dev/null; echo; } >> "$EV/proc_status.txt"
  sleep 2
done
wait "$SH"; STATUS=$?
kill "$VM" "$IO" 2>/dev/null
echo "  atividade2.py terminou com código $STATUS"

echo "[3/5] strace -c (resumo de chamadas de sistema, execução reduzida)"
if command -v strace >/dev/null; then
  strace -f -c -o "$EV/strace_resumo.txt" $PY atividade2.py --skip-llm --embedding-mode hash --corpus-sizes 40 \
    --workers 1 2 --cache-sizes 10 --repetitions 3 --consultas 12 --read-sizes-mb 8 --only concorrencia \
    --grades g1 --output-dir resultados/_strace_tmp --data-dir resultados/_strace_dados > /dev/null 2>&1
  echo "[4/5] strace de leitura (convencional x mmap)"
  strace -f -e trace=openat,read,pread64,mmap,munmap,madvise,fadvise64 -o "$EV/strace_leitura.txt" \
    $PY atividade2.py --only leitura --read-sizes-mb 8 --repetitions 3 --output-dir resultados/_strace_tmp > /dev/null 2>&1
  echo "[5/5] strace de sincronização (futex)"
  strace -f -e trace=futex -o "$EV/strace_sync.txt" \
    $PY atividade2.py --only sincronizacao --repetitions 3 --output-dir resultados/_strace_tmp > /dev/null 2>&1
  rm -rf resultados/_strace_tmp resultados/_strace_dados
else
  echo "strace não encontrado (sudo apt install strace)" > "$EV/strace_resumo.txt"
fi

echo "Gerando gráficos e tabelas"
$PY gerar_graficos.py
echo "Pronto. Evidências em $EV/ e gráficos em resultados/graficos/"
