"""
解码从 DolphinScheduler 直接下载的完整任务日志 (带 [INFO] 前缀 + 无 END 标记).

与 decode_log.py 的区别: 这个能处理 DS 日志前缀 (` -> ` 之后才是 base64),
自动定位 gzip 魔数 H4sI 开头, 忽略前面的 FILE LIST / LOG INFO / INFO 行.

用法:
    python decode_ds_log.py <input_ds_log.txt> <output.log>
"""
import base64
import gzip
import re
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def main():
    if len(sys.argv) != 3:
        print("Usage: python decode_ds_log.py <input_ds_log.txt> <output.log>")
        sys.exit(1)

    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])
    if not in_path.exists():
        print(f"输入文件不存在: {in_path}")
        sys.exit(1)

    raw = in_path.read_text(encoding="utf-8", errors="ignore")

    # gzip base64 一定以 H4sI 开头; 从第一个 H4sI 截到文件末尾.
    idx = raw.find("H4sI")
    if idx == -1:
        print("找不到 gzip base64 开头 (H4sI). 文件可能不是 base64+gzip 格式.")
        sys.exit(1)

    tail = raw[idx:]
    # 只保留 base64 合法字符 (A-Za-z0-9+/=), 去掉换行/空格/可能的尾部标记.
    b64 = re.sub(r"[^A-Za-z0-9+/=]", "", tail)

    try:
        gz = base64.b64decode(b64)
        text = gzip.decompress(gz).decode("utf-8")
    except Exception as e:
        print(f"解码失败: {e}")
        sys.exit(1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    line_count = text.count("\n")
    size_mb = len(text) / 1024 / 1024
    print(f"✓ 解码成功: {line_count:,} 行, {size_mb:.1f} MB → {out_path}")


if __name__ == "__main__":
    main()
