#!/bin/sh
set -e

# 日志轮转大小，默认 100M，可通过 LOG_MAX_SIZE 环境变量覆盖
LOG_MAX_SIZE="${LOG_MAX_SIZE:-100M}"
LOG_ROTATE_INTERVAL="${LOG_ROTATE_INTERVAL:-5min}"

# 渲染 logrotate 配置模板
# sed "s|{{LOG_MAX_SIZE}}|${LOG_MAX_SIZE}|g" \
#     /etc/logrotate.d/podwatcher > /etc/logrotate.d/podwatcher.rendered
# mv /etc/logrotate.d/podwatcher.rendered /etc/logrotate.d/podwatcher

# 将间隔转换为秒数
case "$LOG_ROTATE_INTERVAL" in
    *min)
        MINS="${LOG_ROTATE_INTERVAL%%min}"
        SECS=$((MINS * 60))
        ;;
    *h)
        HOURS="${LOG_ROTATE_INTERVAL%%h}"
        SECS=$((HOURS * 3600))
        ;;
    *)
        SECS=300
        ;;
esac

# 后台循环执行 logrotate（不需要 root / crond）
(
  while true; do
      sleep "$SECS"
      /usr/sbin/logrotate /etc/logrotate.d/podwatcher 2>/dev/null || true
  done
) &

echo "logrotate configured: maxsize=${LOG_MAX_SIZE}, check every ${LOG_ROTATE_INTERVAL}"

# 启动应用
exec ./podwatcher
