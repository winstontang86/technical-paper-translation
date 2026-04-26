"""table_extractor.py —— 从 PDF 中把表格区域截为 PNG 图片。

用途：
    Marker / pymupdf 对复杂表格（多级表头、跨行跨列）还原效果差，
    译文中的表格经常变成乱码。本模块提供"表格即图片"兜底策略：
      1. 用 pdfplumber 找到每个表格的 bbox；
      2. 用 pymupdf 按 bbox 裁剪为 PNG 保存；
      3. 返回截图清单，供 preprocess 在 Markdown 中插入图片引用
         并清除被误抽的 Markdown 表格文本。

设计要点：
    - 完全可选：pdfplumber 未安装或检测失败不会中断主流程。
    - 分块模式友好：支持通过 page_offset 让返回的 page 号对齐原始 PDF 页码。
    - 2x 清晰度截图，适合阅读。
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class TableImage:
    """单张被截图的表格。

    Attributes:
        page: 1-based 页码（相对于被传入的 PDF，或加上 page_offset 后对齐原 PDF）。
        index_on_page: 该页第几个表格（1-based）。
        image_path: 保存后的 PNG 路径（绝对路径）。
        rel_from_outdir: 用于 Markdown 引用的相对路径（相对 outdir，posix 风格）。
        bbox: 原始 PDF 坐标 (x0, top, x1, bottom)，单位 points。
        caption_hint: 若能从上下文猜出 "Table 1: xxx" 则保留，否则 None。
    """
    page: int
    index_on_page: int
    image_path: Path
    rel_from_outdir: str
    bbox: tuple
    caption_hint: Optional[str] = None


def is_available() -> bool:
    """pdfplumber + pymupdf 都可用才返回 True。"""
    try:
        import pdfplumber  # noqa: F401
        import fitz  # noqa: F401
        return True
    except ImportError:
        return False


def _render_bbox_to_png(fitz_doc, page_index_0: int, bbox: tuple, out_path: Path,
                       zoom: float = 2.0, pad: float = 2.0) -> None:
    """用 pymupdf 把给定 PDF 页面中 bbox 区域渲染为 PNG。

    bbox 来自 pdfplumber：(x0, top, x1, bottom)，坐标原点左上角，单位 points。
    pymupdf 的坐标体系与之一致（也是左上原点 points），可直接用作 clip。
    """
    import fitz
    page = fitz_doc[page_index_0]
    x0, top, x1, bottom = bbox
    # 稍微外扩一点，避免边框线被裁掉
    rect = fitz.Rect(max(0, x0 - pad), max(0, top - pad),
                     x1 + pad, bottom + pad)
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(clip=rect, matrix=mat, alpha=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pix.save(str(out_path))


def _guess_caption(plumber_page, bbox: tuple) -> Optional[str]:
    """在表格 bbox 上/下若干 points 内找形如 'Table N: ...' 的短行作为 caption 提示。"""
    import re
    try:
        words = plumber_page.extract_text(x_tolerance=1.5, y_tolerance=1.5) or ""
    except Exception:
        return None
    for line in words.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(Table|表)\s*\d+[\.:：]", line, re.IGNORECASE)
        if m:
            return line[:200]
    return None


def extract_tables_as_images(
    pdf_path: Path,
    out_image_dir: Path,
    outdir_root: Path,
    *,
    page_offset: int = 0,
    image_rel_prefix: str = "assets/tables",
    zoom: float = 2.0,
) -> List[TableImage]:
    """把 pdf_path 中检测到的所有表格截图，保存到 out_image_dir。

    Args:
        pdf_path: 要处理的 PDF（可能是整篇，也可能是分块 PDF）。
        out_image_dir: 图片实际保存目录（绝对路径）。
        outdir_root: 用于计算 rel_from_outdir 的根目录。
        page_offset: 若 pdf_path 是整篇 PDF 的第 k 页起抽出的分块，
            则 page_offset=k-1，使返回的 page 对齐原始 PDF 页码。
        image_rel_prefix: Markdown 中引用时的相对路径前缀（相对 outdir_root）。
        zoom: 截图分辨率倍数，2.0 对应 144 dpi。

    Returns:
        TableImage 列表；若 pdfplumber/pymupdf 不可用或 PDF 无表格，返回空列表。
    """
    if not is_available():
        return []

    import fitz
    try:
        import pdfplumber
    except ImportError:
        return []

    results: List[TableImage] = []
    out_image_dir.mkdir(parents=True, exist_ok=True)

    try:
        fitz_doc = fitz.open(str(pdf_path))
    except Exception as e:
        print(f"[table_extractor] open pdf failed: {e}", file=sys.stderr)
        return []

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for i, plumber_page in enumerate(pdf.pages):
                try:
                    tables = plumber_page.find_tables() or []
                except Exception as e:
                    print(f"[table_extractor] find_tables failed on page {i+1}: {e}",
                          file=sys.stderr)
                    continue
                if not tables:
                    continue
                for t_idx, table in enumerate(tables, start=1):
                    bbox = tuple(float(v) for v in table.bbox)  # (x0, top, x1, bottom)
                    # 过滤明显不是表格的退化区域
                    width = bbox[2] - bbox[0]
                    height = bbox[3] - bbox[1]
                    if width < 30 or height < 15:
                        continue

                    real_page = i + 1 + page_offset
                    img_name = f"table_p{real_page:03d}_{t_idx:02d}.png"
                    img_path = out_image_dir / img_name
                    try:
                        _render_bbox_to_png(fitz_doc, i, bbox, img_path, zoom=zoom)
                    except Exception as e:
                        print(f"[table_extractor] render failed p{real_page} t{t_idx}: {e}",
                              file=sys.stderr)
                        continue

                    caption = _guess_caption(plumber_page, bbox)

                    try:
                        rel = img_path.resolve().relative_to(outdir_root.resolve())
                        rel_posix = rel.as_posix()
                    except ValueError:
                        # image_dir 不在 outdir_root 下：退化为使用前缀 + 文件名
                        rel_posix = f"{image_rel_prefix.rstrip('/')}/{img_name}"

                    results.append(TableImage(
                        page=real_page,
                        index_on_page=t_idx,
                        image_path=img_path,
                        rel_from_outdir=rel_posix,
                        bbox=bbox,
                        caption_hint=caption,
                    ))
    finally:
        fitz_doc.close()

    return results


def strip_markdown_tables(md_text: str) -> str:
    """删除 Markdown 中形如 `|---|` 表格块，并在原位置留一个占位注释。

    用于 table_strategy=image 模式：我们已经把表格截成图片，
    原 Markdown 中 Marker 抽出的（通常错乱的）文本表格应被清除，
    避免译文里既有图又有一堆乱字。
    """
    import re
    table_re = re.compile(
        r"(?:^\|[^\n]*\|\s*\n)+^\|[ :\-|]+\|\s*\n(?:^\|[^\n]*\|\s*\n?)+",
        re.MULTILINE,
    )
    return table_re.sub("<!-- table removed: rendered as image -->\n\n", md_text)


def build_markdown_image_block(ti: TableImage) -> str:
    """为单张表格图片构造 Markdown 图片块（含可选 caption 提示）。"""
    alt = ti.caption_hint or f"Table p{ti.page}-{ti.index_on_page}"
    # Markdown 中不能有回车和反括号干扰图片语法
    alt = alt.replace("]", ")").replace("\n", " ").strip()
    return f"\n\n![{alt}]({ti.rel_from_outdir})\n\n"
