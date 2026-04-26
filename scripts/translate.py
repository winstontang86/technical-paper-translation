"""translate.py —— 按翻译单元生成滑动窗口 prompt，并收集译文。

设计：
  本 skill 不直接绑定任何 LLM API 或特定 Agent 平台。它把每个翻译单元的请求写成
  结构化 prompt 文件，任何宿主平台或外部 LLM 执行器只要能读取 prompt、调用模型、
  并把译文写回约定路径，就可以完成翻译流程。

支持三种翻译单元：
  segment：逐段翻译，最稳。
  section：逐章节翻译，最快。
  hybrid：短章节整章翻译，长章节按段落边界拆成章节 part，推荐默认提速模式。

断点续译：
  collect 阶段遇到空 zh.md 会警告；generate 阶段不会覆盖已有非空 zh.md。
  generate 会优先把上一翻译单元已完成的中文译文放入 previous_zh_context；若不存在则回退上一单元英文原文。
"""
from __future__ import annotations

import re
import sys
import json
import argparse
from pathlib import Path
from typing import List, Dict, Any


# 窗口大小（字符数，近似 token * 4）
PREV_WINDOW_CHARS = 800 * 4
NEXT_WINDOW_CHARS = 800 * 4
HYBRID_MAX_CHARS = 12000


def load_glossary(builtin_path: Path, user_path: Path | None) -> Dict[str, str]:
    def _load(p: Path) -> Dict[str, str]:
        data = json.loads(p.read_text(encoding="utf-8"))
        return {k: v for k, v in data.items() if not k.startswith("_")}

    glossary: Dict[str, str] = {}
    if builtin_path.exists():
        glossary.update(_load(builtin_path))
    if user_path and Path(user_path).exists():
        glossary.update(_load(Path(user_path)))
    return glossary


def _filter_glossary_for_text(glossary: Dict[str, str], text: str) -> List[str]:
    """仅保留在 text 中出现的术语，减少 prompt 长度。"""
    lines = []
    text_lower = text.lower()
    for en, zh in glossary.items():
        # 简单匹配：忽略大小写；单词边界
        key = en.lower()
        if re.search(r"\b" + re.escape(key) + r"\b", text_lower):
            lines.append(f"{en} -> {zh}")
    return lines


def _assign_waves(units: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """为每个翻译单元注入 wave 字段，用于外部执行器并行调度。

    规则：
    - is_reference=True：wave=-1（不翻译）。
    - 同一 section_heading 内的单元必须串行（因为后面的 previous_zh 依赖前面）：
      在同一 section 内按顺序 wave 递增。
    - 不同 section 之间同一 wave 可并行：某个 wave 可包含多个不同 section 的单元。
    - 同 wave 内的单元彼此没有 previous_zh 依赖，可安全并发翻译。
    """
    section_idx: Dict[str, int] = {}
    for unit in units:
        if unit.get("is_reference"):
            unit["wave"] = -1
            continue
        heading = unit.get("section_heading", "")
        idx = section_idx.get(heading, 0)
        unit["wave"] = idx
        section_idx[heading] = idx + 1
    return units


def _segment_to_unit(seg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": seg["id"],
        "kind": "segment",
        "segment_ids": [seg["id"]],
        "section_heading": seg.get("section_heading", ""),
        "section_level": seg.get("section_level", 0),
        "text": seg["text"],
        "is_reference": bool(seg.get("is_reference")),
        "char_len": seg.get("char_len", len(seg["text"])),
    }


def _flush_unit(units: List[Dict[str, Any]], section_idx: int, part_idx: int,
                section: Dict[str, Any], bucket: List[Dict[str, Any]]) -> None:
    if not bucket:
        return
    unit_id = f"sec_{section_idx:04d}" if part_idx == 1 else f"sec_{section_idx:04d}_part_{part_idx:03d}"
    text = "\n\n".join(seg["text"] for seg in bucket)
    units.append({
        "id": unit_id,
        "kind": "section" if len(bucket) > 1 else "segment",
        "segment_ids": [seg["id"] for seg in bucket],
        "section_heading": section["heading"],
        "section_level": section["level"],
        "text": text,
        "is_reference": all(bool(seg.get("is_reference")) for seg in bucket),
        "char_len": len(text),
    })


def build_translation_units(segments: List[Dict[str, Any]], unit_mode: str = "segment",
                            hybrid_max_chars: int = HYBRID_MAX_CHARS) -> List[Dict[str, Any]]:
    """把 segment 列表转换为翻译单元。"""
    if unit_mode not in {"segment", "section", "hybrid"}:
        raise ValueError(f"Unknown unit_mode: {unit_mode}")
    if unit_mode == "segment":
        return [_segment_to_unit(seg) for seg in segments]

    sections: List[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None
    for seg in segments:
        key = (seg.get("section_heading", ""), seg.get("section_level", 0))
        if current is None or current["key"] != key:
            current = {
                "key": key,
                "heading": seg.get("section_heading", ""),
                "level": seg.get("section_level", 0),
                "segments": [],
            }
            sections.append(current)
        current["segments"].append(seg)

    units: List[Dict[str, Any]] = []
    for section_idx, section in enumerate(sections, start=1):
        sec_segments = section["segments"]
        if unit_mode == "section":
            _flush_unit(units, section_idx, 1, section, sec_segments)
            continue

        part_idx = 1
        bucket: List[Dict[str, Any]] = []
        used = 0
        for seg in sec_segments:
            seg_len = seg.get("char_len", len(seg["text"]))
            if bucket and used + seg_len + 2 > hybrid_max_chars:
                _flush_unit(units, section_idx, part_idx, section, bucket)
                part_idx += 1
                bucket = []
                used = 0
            bucket.append(seg)
            used += seg_len + 2
        _flush_unit(units, section_idx, part_idx, section, bucket)
    return units


def _read_existing_zh(zh_dir: Path, unit_id: str) -> str:
    zh_path = zh_dir / f"{unit_id}.zh.md"
    if not zh_path.exists():
        return ""
    return zh_path.read_text(encoding="utf-8").strip()


def _build_window(units: List[Dict[str, Any]], idx: int, zh_dir: Path) -> Dict[str, str]:
    """为第 idx 个翻译单元构建 previous_zh / previous_source / next_source 窗口；不跨越 section。"""
    cur = units[idx]
    cur_section = cur.get("section_heading", "")

    def same_section(i: int) -> bool:
        return units[i].get("section_heading", "") == cur_section and not units[i].get("is_reference")

    previous_zh = ""
    previous_source = ""
    if idx > 0 and same_section(idx - 1):
        previous_zh = _read_existing_zh(zh_dir, units[idx - 1]["id"])
        previous_source = units[idx - 1]["text"]
        if len(previous_zh) > PREV_WINDOW_CHARS:
            previous_zh = previous_zh[-PREV_WINDOW_CHARS:]
        if len(previous_source) > PREV_WINDOW_CHARS:
            previous_source = previous_source[-PREV_WINDOW_CHARS:]

    next_pieces = []
    used = 0
    i = idx + 1
    while i < len(units) and used < NEXT_WINDOW_CHARS:
        if not same_section(i):
            break
        piece = units[i]["text"]
        if used + len(piece) > NEXT_WINDOW_CHARS:
            piece = piece[:NEXT_WINDOW_CHARS - used]
        next_pieces.append(piece)
        used += len(piece)
        i += 1

    return {
        "previous_zh": previous_zh,
        "previous_source": previous_source,
        "next_source": "\n\n".join(next_pieces).strip(),
    }


SYSTEM_PROMPT = """你是专业的技术论文译者，擅长 AI/机器学习领域英文论文的中文翻译。严格遵循以下规则：

【风格】信达雅，学术书面语，第三人称视角；忠实于原文，不得省略、不得自行概括、不得补全原文没有的内容。一段对应一段，逐句翻译。

【结构保真】
- Markdown 结构（标题 # 层级、列表、引用块）一比一保留。
- 文本中的占位符形如 ⟦CODE_0001⟧、⟦FORMULA_0003⟧、⟦TABLE_0002⟧、⟦IMAGE_0005⟧、⟦INLINE_FORMULA_0004⟧，这些都代表被锁定的内容（公式/代码/表格/图片），必须原样保留在译文对应位置，不得改动，不得翻译。
- 如果 Markdown 表格没有被替换成 ⟦TABLE_0001⟧ 这类占位符，必须一比一保留表格列数、分隔线、行数、数字、单位和变量符号，只翻译自然语言文字单元格。
- 引用编号 [12]、[Author, 2024]、(Smith et al., 2023) 原样保留。

【术语】
- 严格使用 <glossary> 中给出的对照；同一术语全文一致。
- 术语首次出现："中文（English）"；后续只用中文。
- 专有名词（模型名、机构名、人名、数据集名）保留英文原样。
- 缩写首次出现："中文全称（英文缩写）"；后续只用缩写。

【上下文】<previous_zh_context> 是上一翻译单元已完成的中文译文，优先用于延续术语、语气和指代；<previous_source_fallback> 与 <next_source_context> 仅用于理解上下文，不得翻译。只输出 <current_source> 的中文译文。

【数字、单位、日期】保留原格式；单位不译；年份日期保留原写法。

【输出格式】直接输出译文，不要添加任何前缀、后缀、解释、"以下是翻译"等字样。不输出 XML 标签。中英文之间加空格，中文用全角标点，英文保留半角标点。
"""


USER_TEMPLATE = """<previous_zh_context source="{previous_zh_source}">
{previous_zh}
</previous_zh_context>

<previous_source_fallback>
{previous_source}
</previous_source_fallback>

<current_source id="{unit_id}" segments="{segment_ids}">
{current}
</current_source>

<next_source_context>
{next_source}
</next_source_context>

<glossary>
{glossary}
</glossary>

请翻译 <current_source> 的内容为中文。仅输出译文本身，不要输出上下文和任何解释。

执行前检查：如果 {previous_zh_source} 存在且非空，请先读取它，并用其内容替换 <previous_zh_context> 中的占位说明；该中文上下文只用于延续术语、语气和指代，不得重复翻译。
"""


def generate(segments_path: Path, outdir: Path, glossary: Dict[str, str],
             unit_mode: str = "segment", hybrid_max_chars: int = HYBRID_MAX_CHARS) -> None:
    segments: List[Dict[str, Any]] = json.loads(segments_path.read_text(encoding="utf-8"))
    units = build_translation_units(segments, unit_mode=unit_mode, hybrid_max_chars=hybrid_max_chars)
    units = _assign_waves(units)
    (outdir / "translation_units.json").write_text(
        json.dumps(units, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    prompts_dir = outdir / "prompts_per_segment"
    zh_dir = outdir / "zh_per_segment"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    zh_dir.mkdir(parents=True, exist_ok=True)

    # 统计 wave 分布供索引展示
    from collections import Counter
    wave_counter: Counter = Counter(u["wave"] for u in units if u.get("wave", -1) >= 0)
    max_wave = max(wave_counter.keys()) if wave_counter else -1
    total_translatable = sum(wave_counter.values())
    max_parallel = max(wave_counter.values()) if wave_counter else 0

    index_lines = [
        "# 翻译任务索引",
        "",
        "## 给外部 LLM 执行器的说明",
        "",
        f"- 当前翻译单元模式：`{unit_mode}`。",
        "- 本目录采用平台中立的文件协议，不依赖 WorkBuddy、OpenClaw 或任何特定宿主。",
        "- 对每个翻译单元 unit_id：",
        "  1. 读取 `prompts_per_segment/<unit_id>.prompt.md`。",
        "  2. 将 `# SYSTEM` 部分作为 system prompt；将 `# USER` 部分作为 user message。若宿主平台不支持 system 角色，可把 system 内容放在 user message 开头。",
        "  3. 将 LLM 输出的中文译文写入 `zh_per_segment/<unit_id>.zh.md`（覆盖写）。",
        "  4. is_reference=True 的单元位于 References / Bibliography 及其后，已从最终译文中排除，无需翻译。",
        "",
        "## 性能：wave 并行调度（推荐）",
        "",
        f"- 共 {total_translatable} 个待翻译单元，共 {max_wave + 1 if max_wave >= 0 else 0} 个 wave；单 wave 最大并行度 = {max_parallel}。",
        "- 同一 wave 内的单元彼此**没有上下文依赖**，可安全并发调用 LLM；wave 之间必须串行（后面的 wave 需要前一 wave 的 `zh.md` 作为 previous_zh_context）。",
        "- 推荐实现：",
        "  ```",
        "  for wave in sorted(unique_waves):",
        "      parallel_run(units where unit.wave == wave, concurrency=N)",
        "  ```",
        "- 如果宿主不支持并发，也可直接按 `unit_id` 顺序串行翻译，两种策略同等正确。",
        "",
        "## 后续步骤",
        "",
        "全部翻译完后，执行：",
        "  `python3 translate.py --mode collect --workdir <outdir>` 组装译文。",
        "",
        "## 翻译单元清单",
        "",
        "| unit_id | wave | segments | section | char_len | is_reference | status |",
        "|---|---|---|---|---|---|---|",
    ]

    for idx, unit in enumerate(units):
        unit_id = unit["id"]
        zh_path = zh_dir / f"{unit_id}.zh.md"
        prompt_path = prompts_dir / f"{unit_id}.prompt.md"
        wave = unit.get("wave", -1)
        wave_display = "-" if wave < 0 else str(wave)

        if unit.get("is_reference"):
            index_lines.append(
                f"| {unit_id} | {wave_display} | {', '.join(unit['segment_ids'])} | {unit['section_heading']} | {unit['char_len']} | yes | excluded |"
            )
            continue

        window = _build_window(units, idx, zh_dir)
        previous_unit_id = ""
        if idx > 0 and units[idx - 1].get("section_heading", "") == unit.get("section_heading", "") and not units[idx - 1].get("is_reference"):
            previous_unit_id = units[idx - 1]["id"]
        previous_zh_source = f"zh_per_segment/{previous_unit_id}.zh.md" if previous_unit_id else "（无同章节上一翻译单元）"
        gloss_lines = _filter_glossary_for_text(
            glossary,
            unit["text"] + "\n" + window["previous_zh"] + "\n" + window["previous_source"] + "\n" + window["next_source"],
        )
        glossary_text = "\n".join(gloss_lines) if gloss_lines else "（无需特别关注的术语）"

        user_msg = USER_TEMPLATE.format(
            previous_zh_source=previous_zh_source,
            previous_zh=window["previous_zh"] or "（上一翻译单元暂无中文译文，翻译执行前若 previous_zh_context source 指向的文件已存在，请读取该文件内容作为中文上下文；否则使用 previous_source_fallback 辅助理解）",
            previous_source=window["previous_source"] or "（本单元为文档开头或章节开头）",
            unit_id=unit_id,
            segment_ids=", ".join(unit["segment_ids"]),
            current=unit["text"],
            next_source=window["next_source"] or "（本单元为文档末尾或章节末尾）",
            glossary=glossary_text,
        )

        prompt_md = (
            f"<!-- wave={wave} unit_id={unit_id} -->\n\n"
            "# SYSTEM\n\n"
            + SYSTEM_PROMPT
            + "\n\n---\n\n# USER\n\n"
            + user_msg
        )
        prompt_path.write_text(prompt_md, encoding="utf-8")

        if not zh_path.exists():
            zh_path.write_text("", encoding="utf-8")

        status = "done" if zh_path.read_text(encoding="utf-8").strip() else "pending"
        index_lines.append(
            f"| {unit_id} | {wave_display} | {', '.join(unit['segment_ids'])} | {unit['section_heading']} | {unit['char_len']} | no | {status} |"
        )

    (outdir / "INDEX.md").write_text("\n".join(index_lines), encoding="utf-8")

    # 额外输出 waves.json，方便外部执行器按 wave 并发调度。
    waves_map: Dict[str, List[str]] = {}
    for u in units:
        if u.get("is_reference"):
            continue
        waves_map.setdefault(str(u["wave"]), []).append(u["id"])
    (outdir / "waves.json").write_text(
        json.dumps(
            {
                "total_units": total_translatable,
                "num_waves": (max_wave + 1) if max_wave >= 0 else 0,
                "max_parallel": max_parallel,
                "waves": {k: waves_map[k] for k in sorted(waves_map, key=lambda x: int(x))},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _load_units_for_collect(segments_path: Path, outdir: Path) -> List[Dict[str, Any]]:
    units_path = outdir / "translation_units.json"
    if units_path.exists():
        return json.loads(units_path.read_text(encoding="utf-8"))
    segments: List[Dict[str, Any]] = json.loads(segments_path.read_text(encoding="utf-8"))
    return [_segment_to_unit(seg) for seg in segments]


def collect(segments_path: Path, outdir: Path) -> Path:
    units = _load_units_for_collect(segments_path, outdir)
    zh_dir = outdir / "zh_per_segment"

    pieces = []
    missing = []
    for unit in units:
        if unit.get("is_reference"):
            continue
        zh_path = zh_dir / f"{unit['id']}.zh.md"
        if not zh_path.exists() or not zh_path.read_text(encoding="utf-8").strip():
            missing.append(unit["id"])
            pieces.append(f"\n[!MISSING TRANSLATION: {unit['id']}]\n\n" + unit["text"])
            continue
        pieces.append(zh_path.read_text(encoding="utf-8").rstrip())

    translated = "\n\n".join(pieces).strip() + "\n"
    out_path = outdir / "translated_raw.md"
    out_path.write_text(translated, encoding="utf-8")

    if missing:
        print(f"[translate.collect] WARNING: {len(missing)} translation units missing: "
              f"{', '.join(missing[:10])}{'...' if len(missing) > 10 else ''}",
              file=sys.stderr)
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["generate", "collect"])
    ap.add_argument("--segments", help="segments.json path")
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--glossary-builtin", default=None)
    ap.add_argument("--glossary-user", default=None)
    ap.add_argument("--unit-mode", choices=["segment", "section", "hybrid"], default="hybrid")
    ap.add_argument("--hybrid-max-chars", type=int, default=HYBRID_MAX_CHARS)
    args = ap.parse_args()

    workdir = Path(args.workdir)
    segments_path = Path(args.segments) if args.segments else (workdir / "segments.json")

    if args.mode == "generate":
        builtin = Path(args.glossary_builtin) if args.glossary_builtin else (
            Path(__file__).parent.parent / "references" / "glossary_ai_ml.json"
        )
        glossary = load_glossary(builtin, Path(args.glossary_user) if args.glossary_user else None)
        generate(segments_path, workdir, glossary,
                 unit_mode=args.unit_mode, hybrid_max_chars=args.hybrid_max_chars)
        print(f"[translate] generated prompts under {workdir}/prompts_per_segment/")
    else:
        out = collect(segments_path, workdir)
        print(out)


if __name__ == "__main__":
    main()
