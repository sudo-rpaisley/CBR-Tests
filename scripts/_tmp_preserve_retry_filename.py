from pathlib import Path

for path in (Path("run_batch.py"), Path("tests/test_result_layout.py"), Path("docs/batch_execution.md")):
    text = path.read_text(encoding="utf-8")
    text = text.replace('f"retry_{attempt:02d}.json"', 'f"retry{attempt:02d}.json"')
    text = text.replace('"retry_03.json"', '"retry03.json"')
    text = text.replace('retry_02.json', 'retry02.json')
    path.write_text(text, encoding="utf-8")
