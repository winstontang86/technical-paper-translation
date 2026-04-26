"""preprocess.py —— 将 PDF / HTML / Markdown 转换为结构化 Markdown。

策略：
- PDF：小文件优先整篇 marker-pdf；大文件按页分块 marker-pdf，分块可并行。
  Marker 超时或失败时回退到 pymupdf 轻量提取，避免大文件卡住。
- HTML：markdownify + BeautifulSoup 清洗后转 Markdown。
- Markdown：直接返回原文。

性能相关：
- --chunk-concurrency：分块 Marker 的并行度；每个 worker 都会加载一份模型，
  内存占用随并行度线性上升，一般 2 个 worker 在 16GB 机器上安全。
- 每个分块会写入 status.json 记录使用的引擎（marker/pymupdf/skip/failed），
  --resume 可选地配合 --retry-fallback 仅重跑之前回退到 pymupdf 的分块。

输出：
- <outdir>/source.md        —— 结构化原文 Markdown
- <outdir>/assets/          —— 抽取的图片（如有）
- <outdir>/preprocess_chunks/<chunk_id>/status.json —— 每个分块的状态
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from table_extractor import (
    TableImage,
    build_markdown_image_block,
    extract_tables_as_images,
    strip_markdown_tables,
)


def _inject_table_images(
    md_text: str,
    tables: list[TableImage],
    *,
    remove_existing_tables: bool = True,
) -> str:
    """把截图出的表格以 Markdown 图片形式插入到文本里，并（可选）删除原有 Markdown 表格。

    策略：
      1. 先按现有 Markdown 表格块进行替换（避免既有截图又有乱表）。
      2. 按 page 升序把所有表格图片追加到文末的 "附：表格截图" 区域。
         （想在页内就地插入需要准确的页锚；Marker 输出不保留页号，追加章节更稳妥。）
    """
    if not tables:
        return md_text
    text = strip_markdown_tables(md_text) if remove_existing_tables else md_text
    blocks = ["\n\n<!-- tables rendered as images below -->"]
    for ti in sorted(tables, key=lambda t: (t.page, t.index_on_page)):
        blocks.append(build_markdown_image_block(ti))
    return text.rstrip() + "\n" + "".join(blocks) + "\n"


def _dehyphenate(text: str) -> str:
    """修复跨行连字符：'hyphen-\nation' -> 'hyphenation'。保留真正的复合词连字符。"""
    # 仅处理 "word-\nword" 这类跨行形式
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    # 多余的行首行尾空白
    text = re.sub(r"[ \t]+\n", "\n", text)
    return text


def _strip_repeating_headers(md: str) -> str:
    """尝试剥离页眉页脚：跨多页重复出现的短行。"""
    lines = md.split("\n")
    # 统计短行（<= 60 字符，非空）的出现次数
    from collections import Counter
    short_lines = [ln.strip() for ln in lines if 0 < len(ln.strip()) <= 60]
    cnt = Counter(short_lines)
    # 出现 >= 3 次且不像正文（不以标点结尾、不是标题）的，判为页眉页脚
    repeats = {
        s for s, c in cnt.items()
        if c >= 3
        and not s.endswith((".", "。", "!", "?", "？", "！", ":", "："))
        and not s.startswith("#")
        and not re.match(r"^\d+\.\s", s)
    }
    if not repeats:
        return md
    out = [ln for ln in lines if ln.strip() not in repeats]
    return "\n".join(out)


def _clean_text(text: str) -> str:
    text = _dehyphenate(text)
    text = _strip_repeating_headers(text)
    return text.strip() + "\n"


def _pdf_page_count(pdf_path: Path) -> int:
    try:
        import fitz  # pymupdf
    except ImportError as e:
        raise ImportError("pymupdf not installed. Run: pip install pymupdf") from e

    doc = fitz.open(str(pdf_path))
    try:
        return doc.page_count
    finally:
        doc.close()


def _copy_assets(src_assets: Path, dst_assets: Path) -> None:
    if not src_assets.exists():
        return
    for src in src_assets.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(src_assets)
        dst = dst_assets / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _rewrite_image_links(md: str, assets_prefix: str = "assets") -> str:
    """修正 Marker/转换输出中的裸图片名或以 `./` 开头的图片引用，
    给它们加上 `assets_prefix/` 前缀，避免 Markdown/Word 渲染时找不到图档。

    已经包含 `assets/`, `http://`, `https://`, `data:`, `/` 绝对路径的不动。
    """
    def repl(match: re.Match[str]) -> str:
        alt = match.group(1)
        raw = match.group(2).strip()
        if re.match(r"^(https?:|data:|/|#)", raw):
            return match.group(0)
        raw = raw.removeprefix("./")
        # 已经包含预期前缀 -> 不重写
        if raw.startswith(f"{assets_prefix}/") or raw.startswith(f"{assets_prefix}\\"):
            return match.group(0)
        return f"![{alt}]({assets_prefix}/{raw})"

    return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", repl, md)


def _prefix_markdown_image_links(md: str, prefix: str) -> str:
    """将 chunk 内相对图片链接改写到 assets/<chunk>/ 下。"""
    def repl(match: re.Match[str]) -> str:
        alt = match.group(1)
        raw = match.group(2).strip()
        if re.match(r"^(https?:|data:|/)", raw):
            return match.group(0)
        raw = raw.removeprefix("./")
        raw = raw.removeprefix("assets/")
        return f"![{alt}](assets/{prefix}/{raw})"

    return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", repl, md)


def _write_marker_output(pdf_path: Path, outdir: Path) -> Path:
    """在当前进程执行 Marker。仅供隔离子进程调用。"""
    try:
        from marker.converters.pdf import PdfConverter
        from marker.models import create_model_dict
        from marker.output import text_from_rendered
    except ImportError as e:
        raise ImportError(
            "marker-pdf not installed. Run: pip install marker-pdf"
        ) from e

    outdir.mkdir(parents=True, exist_ok=True)
    converter = PdfConverter(artifact_dict=create_model_dict())
    rendered = converter(str(pdf_path))
    text, _, images = text_from_rendered(rendered)

    assets_dir = outdir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    if isinstance(images, dict):
        for name, img in images.items():
            img_path = assets_dir / name
            img_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                if hasattr(img, "save"):
                    img.save(img_path)
                elif isinstance(img, (bytes, bytearray)):
                    img_path.write_bytes(bytes(img))
            except Exception as e:
                print(f"[preprocess] save image failed: {name} -> {e}", file=sys.stderr)

    md_path = outdir / "source.md"
    md_path.write_text(_clean_text(text), encoding="utf-8")
    return md_path


def _run_marker_subprocess(pdf_path: Path, workdir: Path, timeout: int, label: str) -> Path:
    """用子进程运行 Marker，超时后可安全终止。"""
    workdir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--marker-worker",
        "--input",
        str(pdf_path),
        "--outdir",
        str(workdir),
    ]
    print(f"[preprocess] marker start label={label} timeout={timeout}s")
    proc = subprocess.Popen(cmd)
    started = time.monotonic()
    next_heartbeat = started + 30

    while True:
        rc = proc.poll()
        if rc is not None:
            if rc != 0:
                raise RuntimeError(f"marker failed label={label} exit={rc}")
            md_path = workdir / "source.md"
            if not md_path.exists() or md_path.stat().st_size == 0:
                raise RuntimeError(f"marker produced empty output label={label}")
            print(f"[preprocess] marker done label={label}")
            return md_path

        elapsed = time.monotonic() - started
        if timeout > 0 and elapsed > timeout:
            proc.kill()
            proc.wait()
            raise TimeoutError(f"marker timeout label={label} after {timeout}s")

        if time.monotonic() >= next_heartbeat:
            print(f"[preprocess] marker running label={label} elapsed={int(elapsed)}s timeout={timeout}s")
            next_heartbeat += 30
        time.sleep(1)


def preprocess_pdf_marker(
    pdf_path: Path,
    outdir: Path,
    timeout: int = 900,
    *,
    table_strategy: str = "image",
) -> Path:
    """Use marker-pdf to convert PDF to Markdown in an isolated subprocess.

    table_strategy=image 时额外调用 pdfplumber 把复杂表格截为 PNG，
    在 Markdown 中替换掉原有 Markdown 表格块、以图片形式保留。
    """
    tmp = outdir / "_marker_full"
    if tmp.exists():
        shutil.rmtree(tmp)
    md_path = _run_marker_subprocess(pdf_path, tmp, timeout, "full")

    # Marker 输出的 Markdown 里图片引用通常是裸文件名（与图片同目录），
    # 但图片会被拷贝到 outdir/assets/ 下，因此需要把链接改写为 assets/<name>。
    text = md_path.read_text(encoding="utf-8")
    text = _rewrite_image_links(text, "assets")

    _copy_assets(tmp / "assets", outdir / "assets")

    if table_strategy == "image":
        try:
            tables = extract_tables_as_images(
                pdf_path,
                out_image_dir=outdir / "assets" / "tables",
                outdir_root=outdir,
            )
            if tables:
                print(f"[preprocess] table_strategy=image: captured {len(tables)} tables as PNG")
                text = _inject_table_images(text, tables)
            else:
                print("[preprocess] table_strategy=image: no tables detected")
        except Exception as e:
            print(f"[preprocess] table extraction failed ({e}); keep Markdown tables as-is",
                  file=sys.stderr)

    final_md = outdir / "source.md"
    final_md.write_text(text, encoding="utf-8")
    return final_md


def _extract_pdf_pages(src_pdf: Path, dst_pdf: Path, start_page: int, end_page: int) -> Path:
    """抽取 1-based 闭区间页码到新的 PDF。"""
    try:
        import fitz  # pymupdf
    except ImportError as e:
        raise ImportError("pymupdf not installed. Run: pip install pymupdf") from e

    src = fitz.open(str(src_pdf))
    dst = fitz.open()
    try:
        dst.insert_pdf(src, from_page=start_page - 1, to_page=end_page - 1)
        dst_pdf.parent.mkdir(parents=True, exist_ok=True)
        dst.save(str(dst_pdf))
    finally:
        dst.close()
        src.close()
    return dst_pdf


def preprocess_pdf_fallback(
    pdf_path: Path,
    outdir: Path,
    *,
    start_page: int | None = None,
    end_page: int | None = None,
    asset_prefix: str = "",
    output_name: str = "source.md",
) -> Path:
    """Fallback: pymupdf light extraction. Page range uses 1-based inclusive pages."""
    try:
        import fitz  # pymupdf
    except ImportError as e:
        raise ImportError("pymupdf not installed. Run: pip install pymupdf") from e

    assets_dir = outdir / "assets"
    if asset_prefix:
        assets_dir = assets_dir / asset_prefix
    assets_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(str(pdf_path))
    chunks = []
    try:
        first = start_page or 1
        last = end_page or doc.page_count
        for page_no in range(first, last + 1):
            page = doc[page_no - 1]
            text = page.get_text("text")
            chunks.append(text)
            for img_idx, img in enumerate(page.get_images(full=True)):
                xref = img[0]
                try:
                    pix = fitz.Pixmap(doc, xref)
                    if pix.n - pix.alpha >= 4:
                        pix = fitz.Pixmap(fitz.csRGB, pix)
                    img_path = assets_dir / f"page{page_no}_img{img_idx + 1}.png"
                    pix.save(str(img_path))
                    rel = img_path.relative_to(outdir)
                    chunks.append(f"\n![figure]({rel.as_posix()})\n")
                    pix = None
                except Exception:
                    pass
    finally:
        doc.close()

    md_path = outdir / output_name
    md_path.write_text(_clean_text("\n\n".join(chunks)), encoding="utf-8")
    return md_path


def _read_chunk_status(chunk_dir: Path) -> dict:
    p = chunk_dir / "status.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_chunk_status(chunk_dir: Path, status: dict) -> None:
    chunk_dir.mkdir(parents=True, exist_ok=True)
    (chunk_dir / "status.json").write_text(
        json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _process_one_chunk(
    pdf_path_str: str,
    chunk_pdf_str: str,
    chunk_dir_str: str,
    chunk_id: str,
    start: int,
    end: int,
    chunk_timeout: int,
    chunk_fallback: str,
) -> dict:
    """在独立进程中处理单个分块：抽页 -> Marker -> 失败则 fallback。返回状态 dict。

    顶层函数以便被 ProcessPoolExecutor pickle。
    """
    pdf_path = Path(pdf_path_str)
    chunk_pdf = Path(chunk_pdf_str)
    chunk_dir = Path(chunk_dir_str)
    chunk_md = chunk_dir / "source.md"

    status = {
        "chunk_id": chunk_id,
        "start_page": start,
        "end_page": end,
        "engine": None,
        "error": None,
        "ts": int(time.time()),
    }

    try:
        if not chunk_pdf.exists():
            _extract_pdf_pages(pdf_path, chunk_pdf, start, end)
        try:
            _run_marker_subprocess(chunk_pdf, chunk_dir, chunk_timeout, chunk_id)
            status["engine"] = "marker"
        except Exception as e:
            status["error"] = f"{type(e).__name__}: {e}"
            if chunk_fallback == "fail":
                status["engine"] = "failed"
                _write_chunk_status(chunk_dir, status)
                raise
            if chunk_fallback == "skip":
                print(f"[preprocess] marker failed for {chunk_id} ({e}); skipping chunk", file=sys.stderr)
                chunk_md.write_text("", encoding="utf-8")
                status["engine"] = "skip"
            else:
                print(f"[preprocess] marker failed for {chunk_id} ({e}); falling back to pymupdf", file=sys.stderr)
                preprocess_pdf_fallback(
                    pdf_path,
                    chunk_dir,
                    start_page=start,
                    end_page=end,
                )
                status["engine"] = "pymupdf"
    finally:
        _write_chunk_status(chunk_dir, status)
    return status


def preprocess_pdf_chunked(
    pdf_path: Path,
    outdir: Path,
    *,
    chunk_pages: int = 12,
    chunk_timeout: int = 300,
    chunk_fallback: str = "pymupdf",
    resume: bool = False,
    chunk_concurrency: int = 1,
    retry_fallback: bool = False,
    table_strategy: str = "image",
) -> Path:
    """大 PDF 分块 Marker；单块失败时按策略 fallback。可并行多分块。

    resume=True：已有非空 source.md 的分块默认跳过，不重跑。
    retry_fallback=True：resume 模式下额外重跑之前 engine=pymupdf/skip/failed 的分块。
    chunk_concurrency>=2：使用进程池并行执行；每个 worker 独立加载 Marker 模型。
    """
    total_pages = _pdf_page_count(pdf_path)
    chunks_dir = outdir / "preprocess_chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)

    # 1. 规划所有分块
    plan: list[dict] = []
    for start in range(1, total_pages + 1, chunk_pages):
        end = min(start + chunk_pages - 1, total_pages)
        chunk_id = f"chunk_{((start - 1) // chunk_pages) + 1:03d}_p{start:03d}-p{end:03d}"
        chunk_dir = chunks_dir / chunk_id
        chunk_pdf = chunk_dir / f"{chunk_id}.pdf"
        plan.append({
            "chunk_id": chunk_id,
            "start": start,
            "end": end,
            "chunk_dir": chunk_dir,
            "chunk_pdf": chunk_pdf,
        })

    # 2. 筛出需要执行的分块
    todo: list[dict] = []
    for item in plan:
        chunk_dir = item["chunk_dir"]
        chunk_md = chunk_dir / "source.md"
        status = _read_chunk_status(chunk_dir)

        if resume and chunk_md.exists() and chunk_md.stat().st_size > 0:
            if retry_fallback and status.get("engine") in {"pymupdf", "skip", "failed"}:
                print(f"[preprocess] retry-fallback: rerun {item['chunk_id']} (was engine={status.get('engine')})")
            else:
                print(f"[preprocess] resume chunk exists: {item['chunk_id']} engine={status.get('engine') or 'unknown'}")
                continue

        # 需要（重新）执行：清空目录
        if chunk_dir.exists():
            shutil.rmtree(chunk_dir)
        chunk_dir.mkdir(parents=True, exist_ok=True)
        todo.append(item)

    # 3. 执行：并行 or 串行
    concurrency = max(1, int(chunk_concurrency))
    if todo:
        print(f"[preprocess] chunked marker: total={len(plan)} todo={len(todo)} "
              f"chunk_pages={chunk_pages} chunk_timeout={chunk_timeout}s concurrency={concurrency}")

    if todo and concurrency > 1:
        with ProcessPoolExecutor(max_workers=concurrency) as pool:
            futures = {}
            for item in todo:
                fut = pool.submit(
                    _process_one_chunk,
                    str(pdf_path),
                    str(item["chunk_pdf"]),
                    str(item["chunk_dir"]),
                    item["chunk_id"],
                    item["start"],
                    item["end"],
                    chunk_timeout,
                    chunk_fallback,
                )
                futures[fut] = item["chunk_id"]
            done = 0
            for fut in as_completed(futures):
                cid = futures[fut]
                done += 1
                try:
                    st = fut.result()
                    print(f"[preprocess] chunk done [{done}/{len(todo)}] {cid} engine={st.get('engine')}")
                except Exception as e:
                    print(f"[preprocess] chunk FAILED [{done}/{len(todo)}] {cid}: {e}", file=sys.stderr)
                    if chunk_fallback == "fail":
                        raise
    else:
        for idx, item in enumerate(todo, start=1):
            try:
                st = _process_one_chunk(
                    str(pdf_path),
                    str(item["chunk_pdf"]),
                    str(item["chunk_dir"]),
                    item["chunk_id"],
                    item["start"],
                    item["end"],
                    chunk_timeout,
                    chunk_fallback,
                )
                print(f"[preprocess] chunk done [{idx}/{len(todo)}] {item['chunk_id']} engine={st.get('engine')}")
            except Exception as e:
                print(f"[preprocess] chunk FAILED [{idx}/{len(todo)}] {item['chunk_id']}: {e}", file=sys.stderr)
                if chunk_fallback == "fail":
                    raise

    # 4. 合并所有分块输出
    combined = []
    all_tables: list[TableImage] = []
    for item in plan:
        chunk_dir = item["chunk_dir"]
        chunk_id = item["chunk_id"]
        chunk_md = chunk_dir / "source.md"
        _copy_assets(chunk_dir / "assets", outdir / "assets" / chunk_id)
        text = chunk_md.read_text(encoding="utf-8") if chunk_md.exists() else ""
        text = _prefix_markdown_image_links(text, chunk_id)

        if table_strategy == "image" and item["chunk_pdf"].exists():
            try:
                chunk_tables = extract_tables_as_images(
                    item["chunk_pdf"],
                    out_image_dir=outdir / "assets" / chunk_id / "tables",
                    outdir_root=outdir,
                    page_offset=item["start"] - 1,
                )
                if chunk_tables:
                    print(f"[preprocess] {chunk_id}: captured {len(chunk_tables)} tables as PNG")
                    text = strip_markdown_tables(text)
                    all_tables.extend(chunk_tables)
            except Exception as e:
                print(f"[preprocess] table extraction failed for {chunk_id} ({e})",
                      file=sys.stderr)

        combined.append(
            f"<!-- PDF_CHUNK {chunk_id} pages={item['start']}-{item['end']} -->\n\n{text.strip()}\n"
        )

    merged = "\n\n".join(combined)
    if table_strategy == "image" and all_tables:
        # 分块模式下已经在各自位置 strip 掉了乱 Markdown 表格；
        # 统一在文末追加所有截图，避免穿插到 PDF_CHUNK 注释之间。
        merged = _inject_table_images(merged, all_tables, remove_existing_tables=False)

    final_md = outdir / "source.md"
    final_md.write_text(_clean_text(merged), encoding="utf-8")
    return final_md


def preprocess_html(html_path: Path, outdir: Path) -> Path:
    """HTML -> Markdown via BeautifulSoup + markdownify."""
    from bs4 import BeautifulSoup
    try:
        from markdownify import markdownify as md_convert
    except ImportError as e:
        raise ImportError("markdownify not installed. Run: pip install markdownify") from e

    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    # Remove non-content tags
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()

    # arXiv/ar5iv: main content usually in <article> or <main>
    main = soup.find("article") or soup.find("main") or soup.body or soup
    md = md_convert(
        str(main),
        heading_style="ATX",
        bullets="-",
    )
    md = _dehyphenate(md)

    # Save images referenced (best-effort: we do NOT download remote images in v1;
    # keep URLs as-is so LLM sees image anchors and postprocess preserves them)
    md_path = outdir / "source.md"
    md_path.write_text(md, encoding="utf-8")
    return md_path


def preprocess(
    input_path: Path,
    kind: str,
    outdir: Path,
    *,
    pdf_engine: str = "auto",
    marker_timeout: int = 900,
    large_pdf_pages: int = 20,
    pdf_chunk_pages: int = 12,
    chunk_timeout: int = 300,
    chunk_fallback: str = "pymupdf",
    resume: bool = False,
    chunk_concurrency: int = 1,
    retry_fallback: bool = False,
    table_strategy: str = "image",
) -> Path:
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if kind == "markdown":
        dst = outdir / "source.md"
        shutil.copyfile(input_path, dst)
        return dst

    if kind in ("html", "arxiv_html"):
        return preprocess_html(input_path, outdir)

    if kind == "pdf":
        if pdf_engine == "pymupdf":
            md = preprocess_pdf_fallback(input_path, outdir)
            if table_strategy == "image":
                _apply_table_images_to_existing_md(input_path, outdir, md)
            return md
        if pdf_engine == "marker":
            return preprocess_pdf_marker(
                input_path, outdir, timeout=marker_timeout,
                table_strategy=table_strategy,
            )
        if pdf_engine == "marker-chunked":
            return preprocess_pdf_chunked(
                input_path,
                outdir,
                chunk_pages=pdf_chunk_pages,
                chunk_timeout=chunk_timeout,
                chunk_fallback=chunk_fallback,
                resume=resume,
                chunk_concurrency=chunk_concurrency,
                retry_fallback=retry_fallback,
                table_strategy=table_strategy,
            )

        pages = _pdf_page_count(input_path)
        print(f"[preprocess] pdf pages={pages} engine=auto")
        if pages > large_pdf_pages:
            print(
                f"[preprocess] pages>{large_pdf_pages}; using chunked marker "
                f"chunk_pages={pdf_chunk_pages} chunk_timeout={chunk_timeout}s "
                f"concurrency={chunk_concurrency}"
            )
            return preprocess_pdf_chunked(
                input_path,
                outdir,
                chunk_pages=pdf_chunk_pages,
                chunk_timeout=chunk_timeout,
                chunk_fallback=chunk_fallback,
                resume=resume,
                chunk_concurrency=chunk_concurrency,
                retry_fallback=retry_fallback,
                table_strategy=table_strategy,
            )

        try:
            return preprocess_pdf_marker(
                input_path, outdir, timeout=marker_timeout,
                table_strategy=table_strategy,
            )
        except Exception as e:
            print(f"[preprocess] marker failed ({e}); falling back to pymupdf", file=sys.stderr)
            md = preprocess_pdf_fallback(input_path, outdir)
            if table_strategy == "image":
                _apply_table_images_to_existing_md(input_path, outdir, md)
            return md

    raise ValueError(f"Unknown kind: {kind}")


def _apply_table_images_to_existing_md(pdf_path: Path, outdir: Path, md_path: Path) -> None:
    """在 pymupdf fallback 之后补截表格图片并改写 source.md。"""
    try:
        tables = extract_tables_as_images(
            pdf_path,
            out_image_dir=outdir / "assets" / "tables",
            outdir_root=outdir,
        )
    except Exception as e:
        print(f"[preprocess] table extraction failed ({e})", file=sys.stderr)
        return
    if not tables:
        return
    print(f"[preprocess] table_strategy=image: captured {len(tables)} tables as PNG (fallback)")
    text = md_path.read_text(encoding="utf-8")
    text = _inject_table_images(text, tables)
    md_path.write_text(text, encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--kind", choices=["pdf", "html", "arxiv_html", "markdown"])
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--pdf-engine", choices=["auto", "marker", "pymupdf", "marker-chunked"], default="auto")
    ap.add_argument("--marker-timeout", type=int, default=900)
    ap.add_argument("--large-pdf-pages", type=int, default=20)
    ap.add_argument("--pdf-chunk-pages", type=int, default=12)
    ap.add_argument("--chunk-timeout", type=int, default=300)
    ap.add_argument("--chunk-fallback", choices=["pymupdf", "skip", "fail"], default="pymupdf")
    ap.add_argument("--chunk-concurrency", type=int, default=1,
                    help="parallel workers for chunked marker; each worker loads its own model")
    ap.add_argument("--retry-fallback", action="store_true",
                    help="with --resume, rerun chunks whose previous engine was pymupdf/skip/failed")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--table-strategy", choices=["image", "markdown"], default="image",
                    help="image: crop table regions from PDF as PNG and replace Markdown tables; "
                         "markdown: keep Marker's Markdown tables as-is")
    ap.add_argument("--marker-worker", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.marker_worker:
        path = _write_marker_output(Path(args.input), Path(args.outdir))
    else:
        if not args.kind:
            ap.error("--kind is required unless --marker-worker is used")
        path = preprocess(
            Path(args.input),
            args.kind,
            Path(args.outdir),
            pdf_engine=args.pdf_engine,
            marker_timeout=args.marker_timeout,
            large_pdf_pages=args.large_pdf_pages,
            pdf_chunk_pages=args.pdf_chunk_pages,
            chunk_timeout=args.chunk_timeout,
            chunk_fallback=args.chunk_fallback,
            resume=args.resume,
            chunk_concurrency=args.chunk_concurrency,
            retry_fallback=args.retry_fallback,
            table_strategy=args.table_strategy,
        )
    print(path)


if __name__ == "__main__":
    main()
