# Общий протокол разделения бенчмарк-машины между несколькими агентами.
# Подключается через `. ~/benchlock.sh` в начале скрипта.
#
# Замок один на всю машину: /home/x4mmm/benchmark.lock. Task-specific замки не
# годятся — мешают друг другу не задачи, а нагрузка на процессор и диск.
#
# Долгоживущие процессы (postmaster прежде всего) не должны наследовать
# дескриптор замка, иначе замок переживёт скрипт. Поэтому все внешние команды
# запускаются с закрытым fd 9: `команда 9>&-`.

BENCHLOCK=/home/x4mmm/benchmark.lock

bench_lock() {
  exec 9>"$BENCHLOCK"
  if ! flock -n 9; then
    echo "benchmark host is busy" >&2
    exit 75
  fi
  trap 'flock -u 9 2>/dev/null || true' EXIT
}

bench_preflight() {
  echo "### preflight $(hostname) $(date -u +%FT%TZ)"
  uptime
  ps -eo pid,pcpu,pmem,args --sort=-pcpu 2>/dev/null | head -20
  local busy
  busy=$(ps -eo pcpu,args --sort=-pcpu | awk 'NR>1 && $1>20 {print}' \
         | grep -viE "benchlock|awk|ps -eo" | head -5)
  if [ -n "$busy" ]; then
    echo "### ВНИМАНИЕ: посторонняя нагрузка перед серией:" >&2
    echo "$busy" >&2
  fi
}

bench_postflight() {
  echo "### postflight $(hostname) $(date -u +%FT%TZ)"
  uptime
  ps -eo pid,pcpu,pmem,args --sort=-pcpu 2>/dev/null | head -10
}
