"""
解码从 DolphinScheduler dump 任务复制下来的 base64+gzip 日志.

用法:
    python decode_log.py logs/server_b64.txt logs/server.log
"""
import base64
import gzip
import sys
from pathlib import Path

# Windows 控制台 UTF-8 输出
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def main():
    if len(sys.argv) != 3:
        print("Usage: python decode_log.py <input_b64.txt> <output.log>")
        sys.exit(1)

    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    if not in_path.exists():
        print(f"输入文件不存在: {in_path}")
        sys.exit(1)

    raw = in_path.read_text(encoding="utf-8")

    # 清掉 DolphinScheduler 加的制表符和换行 (复制时会带这些)
    cleaned = "".join(raw.split())

    try:
        gz = base64.b64decode(cleaned)
        text = gzip.decompress(gz).decode("utf-8")
    except Exception as e:
        print(f"解码失败: {e}")
        print("请检查复制下来的 base64 是否完整 (BEGIN 和 END 标记之间的所有内容).")
        sys.exit(1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    line_count = text.count("\n")
    size_kb = len(text) / 1024
    print(f"✓ 解码成功: {line_count} 行, {size_kb:.1f} KB → {out_path}")


if __name__ == "__main__":
    main()
