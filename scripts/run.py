"""run.py —— 总入口。

阶段：
  stage1_prepare: fetch + preprocess + segment + translate.generate
    产出 INDEX.md 与 per-segment prompts，等待外部 LLM 执行器填译文。
  stage2_finalize: translate.collect + postprocess + qa_report
    组装译文、后处理、阻断级质检、产出最终文件。

用法示例：
  # 阶段 1：准备任务
  python3 run.py --input paper.pdf --outdir out/ --stage prepare

  # 阶段 2：外部 LLM 执行器完成翻译后，组装 + 质检
  python3 run.py --outdir out/ --stage finalize

  # 一键模式：当前等价于 prepare，不绑定任何具体 LLM 平台
  python3 run.py --input paper.pdf --outdir out/ --stage all
  （all 模式会生成翻译任务索引；译文写回后再执行 finalize）
"""
from __future__ import annotations

import sys
import argparse
from pathlib import Path

# 允许作为 `python3 run.py` 直接运行
sys.path.insert(0, str(Path(__file__).parent))

from fetch import fetch
from preprocess import preprocess
from segment import run as segment_run
from translate import load_glossary, generate as translate_generate, collect as translate_collect
from postprocess import postprocess
from qa_report import check as qa_check, write_report as qa_write, write_fix_prompts as qa_write_fix_prompts


SKILL_ROOT = Path(__file__).parent.parent


import re
import shutil

_IMG_RE = re.compile(r"(!\[[^\]]*\]\()([^)\n]+)(\))")


def _rewrite_final_image_paths(md_text: str, new_prefix: str) -> str:
    """将文中的 `assets/...` 相对路径改写为 `<new_prefix>/...`。保留绝对路径和 http(s)/data 链接。"""
    def repl(m: re.Match[str]) -> str:
        head, raw, tail = m.group(1), m.group(2).strip(), m.group(3)
        if re.match(r"^(https?:|data:|/|#)", raw):
            return m.group(0)
        raw = raw.removeprefix("./")
        if raw.startswith("assets/"):
            raw = raw[len("assets/"):]
            return f"{head}{new_prefix}/{raw}{tail}"
        return m.group(0)

    return _IMG_RE.sub(repl, md_text)


def _try_export_docx(md_path: Path, docx_path: Path, resource_dirs: list[Path]) -> bool:
    """尝试用 pandoc 将 Markdown 导出为 docx。返回是否成功。"""
    pandoc = shutil.which("pandoc")
    if not pandoc:
        print("[run] pandoc not found; skip docx export. Install with: brew install pandoc")
        return False
    import subprocess
    resource_path = ":".join(str(p) for p in resource_dirs if p)
    cmd = [pandoc, str(md_path), "-o", str(docx_path), "--from=gfm+tex_math_dollars",
           "--standalone"]
    if resource_path:
        cmd += [f"--resource-path={resource_path}"]
    try:
        subprocess.run(cmd, check=True)
        print(f"[run] docx exported: {docx_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[run] pandoc failed: {e}", file=sys.stderr)
        return False


def stem_of(input_str: str) -> str:
    p = Path(input_str)
    if p.exists():
        return p.stem
    # URL
    from urllib.parse import urlparse
    u = urlparse(input_str)
    s = Path(u.path).stem or u.netloc.replace(".", "_")
    return s or "paper"


def stage_prepare(args) -> Path:
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[run] stage=prepare input={args.input}")
    local_path, kind = fetch(args.input, outdir)
    print(f"[run] fetched: kind={kind} path={local_path}")

    source_md = outdir / "source.md"
    if args.resume and source_md.exists() and source_md.stat().st_size > 0:
        print(f"[run] resume: source.md exists; skip preprocess: {source_md}")
    else:
        source_md = preprocess(
            local_path,
            kind,
            outdir,
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
        print(f"[run] preprocessed: {source_md}")

    segments_path = outdir / "segments.json"
    locked_path = outdir / "locked_blocks.json"
    masked_path = outdir / "masked.md"
    if (
        args.resume
        and segments_path.exists()
        and segments_path.stat().st_size > 0
        and locked_path.exists()
        and locked_path.stat().st_size > 0
    ):
        seg_out = {"masked": masked_path, "locked": locked_path, "segments": segments_path}
        print(f"[run] resume: segments.json exists; skip segment: {segments_path}")
    else:
        seg_out = segment_run(source_md, outdir, table_mode=args.table_mode)
        print(f"[run] segmented: {seg_out['segments']} table_mode={args.table_mode}")

    builtin = SKILL_ROOT / "references" / "glossary_ai_ml.json"
    user_glossary = Path(args.glossary) if args.glossary else None
    glossary = load_glossary(builtin, user_glossary)
    translate_generate(
        seg_out["segments"], outdir, glossary,
        unit_mode=args.unit_mode,
        hybrid_max_chars=args.hybrid_max_chars,
    )
    print(f"[run] prompts generated in {outdir}/prompts_per_segment/ unit_mode={args.unit_mode}")
    print(f"[run] INDEX: {outdir}/INDEX.md")
    waves_path = outdir / "waves.json"
    if waves_path.exists():
        print(f"[run] waves plan: {waves_path} (同 wave 内的单元可并发翻译)")
    print()
    print("=" * 60)
    print("下一步：外部 LLM 执行器按 INDEX.md 指引逐个翻译单元翻译。")
    print("推荐调度策略：按 waves.json 中的 wave 顺序，同一 wave 的单元可并行调用 LLM；wave 间必须串行。")
    print("所有翻译单元完成并写回 zh_per_segment/ 后，运行：")
    print(f"  python3 {Path(__file__).name} --stage finalize --outdir {outdir}")
    if args.bilingual:
        print("  （finalize 阶段加 --bilingual 生成双语对照）")
    print("=" * 60)
    return outdir


def stage_finalize(args) -> None:
    outdir = Path(args.outdir)
    segments_path = outdir / "segments.json"
    locked_path = outdir / "locked_blocks.json"
    source_md = outdir / "source.md"
    if not segments_path.exists():
        raise FileNotFoundError(f"missing {segments_path}; run --stage prepare first")

    raw = translate_collect(segments_path, outdir)
    print(f"[run] translated_raw: {raw}")

    stem = stem_of(args.input) if args.input else "translated"
    final_md = outdir / f"{stem}.zh.md"
    postprocess(raw, locked_path, final_md)
    print(f"[run] postprocessed: {final_md}")

    # 同步复制一份 assets 为 <stem>.assets/，并把译文里的 assets/xxx 改写为 <stem>.assets/xxx
    # 这样用户单独拷走 <stem>.zh.md 和 <stem>.assets/、或以其为源转 docx，都能正确找到图片。
    src_assets = outdir / "assets"
    final_assets = outdir / f"{stem}.assets"
    if src_assets.exists():
        if final_assets.exists():
            shutil.rmtree(final_assets)
        shutil.copytree(src_assets, final_assets)
        print(f"[run] assets mirrored: {final_assets}")

        md_text = final_md.read_text(encoding="utf-8")
        new_text = _rewrite_final_image_paths(md_text, f"{stem}.assets")
        if new_text != md_text:
            final_md.write_text(new_text, encoding="utf-8")
            print(f"[run] rewrote image links in {final_md.name}")

    skip = [s.strip() for s in (args.skip_checks or "").split(",") if s.strip()]
    summary, blockers = qa_check(source_md, final_md, segments_path, skip_checks=skip)
    qa_report_path = outdir / f"{stem}.qa.md"
    qa_write(summary, qa_report_path)
    fix_dir = qa_write_fix_prompts(summary, outdir)
    print(f"[run] qa report: {qa_report_path}")
    if fix_dir:
        print(f"[run] fix prompts: {fix_dir}")

    if blockers and not args.force:
        print(f"[run] BLOCKED: {len(blockers)} blocker(s).")
        raise SystemExit(2)

    if args.bilingual:
        import json as _json
        units_path = outdir / "translation_units.json"
        if units_path.exists():
            items = _json.loads(units_path.read_text(encoding="utf-8"))
        else:
            items = _json.loads(segments_path.read_text(encoding="utf-8"))
        zh_dir = outdir / "zh_per_segment"
        bilingual_lines = []
        for item in items:
            if item.get("is_reference"):
                continue
            item_id = item["id"]
            zh_path = zh_dir / f"{item_id}.zh.md"
            zh = zh_path.read_text(encoding="utf-8").strip() if zh_path.exists() else ""
            bilingual_lines.append(f"<!-- {item_id} EN -->")
            bilingual_lines.append(item["text"])
            bilingual_lines.append("")
            bilingual_lines.append(f"<!-- {item_id} ZH -->")
            bilingual_lines.append(zh)
            bilingual_lines.append("")
            bilingual_lines.append("---")
            bilingual_lines.append("")
        bilingual_path = outdir / f"{stem}.bilingual.md"
        bilingual_path.write_text("\n".join(bilingual_lines), encoding="utf-8")
        # 双语文件同样应该能渲染图片：将 assets/ 改写为 <stem>.assets/
        if (outdir / f"{stem}.assets").exists():
            bi_text = bilingual_path.read_text(encoding="utf-8")
            bi_new = _rewrite_final_image_paths(bi_text, f"{stem}.assets")
            if bi_new != bi_text:
                bilingual_path.write_text(bi_new, encoding="utf-8")
        print(f"[run] bilingual: {bilingual_path}")

    if args.export_docx:
        docx_path = outdir / f"{stem}.zh.docx"
        _try_export_docx(
            final_md,
            docx_path,
            resource_dirs=[outdir, outdir / f"{stem}.assets", outdir / "assets"],
        )

    print(f"[run] DONE. Output: {final_md}")


def main():
    ap = argparse.ArgumentParser(description="Technical paper translation pipeline.")
    ap.add_argument("--input", help="PDF path or URL")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--stage", choices=["prepare", "finalize", "all"], default="all")
    ap.add_argument("--glossary", help="User glossary JSON (overrides builtin)")
    ap.add_argument("--bilingual", action="store_true", help="Also emit bilingual .md")
    ap.add_argument("--export-docx", action="store_true",
                    help="finalize 阶段额外导出 .docx（需本机安装 pandoc，包含图片）")
    ap.add_argument("--force", action="store_true", help="Skip blocker checks")
    ap.add_argument("--skip-checks", default="", help="comma-separated Bx to skip")
    ap.add_argument("--resume", action="store_true",
                    help="(prepare) reuse existing source.md, segments.json and completed chunk outputs when possible")
    ap.add_argument("--pdf-engine", choices=["auto", "marker", "pymupdf", "marker-chunked"], default="auto",
                    help="PDF preprocess engine: auto uses full Marker for small PDFs and chunked Marker for large PDFs")
    ap.add_argument("--marker-timeout", type=int, default=900,
                    help="timeout seconds for full-PDF Marker in auto/marker mode")
    ap.add_argument("--large-pdf-pages", type=int, default=20,
                    help="auto mode switches to chunked Marker when PDF pages exceed this threshold")
    ap.add_argument("--pdf-chunk-pages", type=int, default=12,
                    help="pages per chunk for marker-chunked mode")
    ap.add_argument("--chunk-timeout", type=int, default=300,
                    help="timeout seconds for each Marker chunk")
    ap.add_argument("--chunk-fallback", choices=["pymupdf", "skip", "fail"], default="pymupdf",
                    help="fallback policy when a Marker chunk fails or times out")
    ap.add_argument("--chunk-concurrency", type=int, default=1,
                    help="parallel workers for chunked marker (each worker loads ~1-2GB model; 2 is a safe default on 16GB machines)")
    ap.add_argument("--retry-fallback", action="store_true",
                    help="with --resume, rerun chunks whose previous engine was pymupdf/skip/failed")
    ap.add_argument("--unit-mode", choices=["segment", "section", "hybrid"], default="hybrid",
                    help="translation unit: segment is safest, section is fastest, hybrid balances both")
    ap.add_argument("--hybrid-max-chars", type=int, default=12000,
                    help="max chars per unit in hybrid mode")
    ap.add_argument("--table-mode", choices=["lock", "translate"], default="lock",
                    help="when Markdown tables remain (table-strategy=markdown), lock them as placeholders "
                         "or translate cell text while preserving structure")
    ap.add_argument("--table-strategy", choices=["image", "markdown"], default="image",
                    help="image (default): crop each PDF table region as PNG and replace the (often garbled) "
                         "Markdown table with an image reference, guaranteeing table integrity in .md and .docx; "
                         "markdown: keep Marker's Markdown table and let --table-mode decide lock/translate")
    args = ap.parse_args()

    if args.stage in ("prepare", "all"):
        if not args.input:
            ap.error("--input required for prepare/all stage")
        stage_prepare(args)
    if args.stage == "finalize":
        stage_finalize(args)


if __name__ == "__main__":
    main()
