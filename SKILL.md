---
name: technical-paper-translation
version: 0.2.6
description: |
  英文技术论文翻译为中文（信达雅学术风格）。支持本地 PDF 文件与 URL 输入（arXiv
  链接优先抓取 HTML 版本）。针对 AI/ML 论文深度优化，兼容通用技术论文。采用滑动
  窗口翻译单元机制保证上下文连贯、术语一致；支持逐段、逐章节和 hybrid 章节翻译；
  阻断级质检确保正文段落、图片、表格、公式、代码、引用均完整保留。默认从 References /
  Bibliography 开始截断，参考文献及其后内容（如 Appendix）不进入最终译文。
  表格默认采用“截图入文”策略（pdfplumber + pymupdf），避免复杂表格被 Marker 抓成乱码、
  并保证导出的 Markdown / Word 中表格清晰完整。PDF 预处理采用小文件整篇 Marker 超时回退、
  大文件分块 Marker 单块回退的组合策略，支持分块并行 + status.json
  断点修复；翻译调度输出 waves.json，同 wave 内单元可并发调用 LLM。finalize 阶段将图片资源
  镜像为 <stem>.assets/ 并改写译文引用，保证单独搬运 Markdown / 导出为 Word 时图片不破。
  整体流程采用平台中立的文件协议，可在 WorkBuddy、OpenClaw 或其他可读写文件并调用 LLM 的平台运行。
  触发词：翻译论文、翻译技术论文、翻译学术论文、翻译 arxiv、arxiv 翻译、论文汉化、
  paper translation、translate paper、英译中论文、paper to Chinese、学术翻译。
location: user
entrypoint: scripts/run.py
---
# technical-paper-translation

英文技术论文翻译为中文（信达雅学术风格）的 skill。

## 何时使用此 skill

当用户提出以下类型请求时，**立即加载并使用此 skill**：

- 翻译一篇 PDF 论文 / arXiv 论文 / 技术论文
- 提供论文 URL（arxiv.org / openreview / ACL Anthology 等）并要求翻译
- 对论文做中文化 / 汉化处理
- 需要保留图表、公式、引用编号的严格学术翻译
- 类似需求："把这篇 paper 翻一下"、"帮我译成中文"、"这篇 arxiv 能不能汉化"

## 核心原则

1. **信达雅 + 忠实**：学术语气，禁止擅自摘要、省略、补全。
2. **结构保真**：正文标题层级、图片、表格、公式（LaTeX）、代码块、引用编号 `[12]` 均原样保留。
3. **References 截断**：从 `References` / `Bibliography` / `参考文献` 标题开始，后续所有内容（包括 `Appendix`、补充材料等）均不翻译、不进入最终译文。
4. **滑动窗口翻译单元**：支持 `segment`、`section`、`hybrid` 三种模式；翻译时输入 `previous_zh_context + current_source + next_source`，仅译 `current_source`。
5. **术语一致**：内置 AI/ML 术语表 + 支持用户自定义 `glossary.json` 覆盖。
6. **阻断级质检**：正文段落对齐、图片/表格/公式/代码/引用数量一致、锁定块完整、References 后内容未混入译文、长度比正常、无摘要性短语；任一不通过则终止并报告，除非用户明确 `--force` 跳过。
7. **定向返修**：QA 阻断时自动生成 `fix_prompts/`，帮助外部 LLM 执行器精准修复问题翻译单元。
8. **平台中立**：核心脚本只依赖 Python 与文件系统；LLM 调用通过 `prompt -> zh.md` 文件协议完成，不绑定 WorkBuddy、OpenClaw 或特定 API。

## 输入

- **本地 PDF**：`/path/to/paper.pdf`
- **URL**：
  - arXiv（`arxiv.org/abs/xxxx` 或 `arxiv.org/pdf/xxxx`）→ 自动改走 HTML 版（ar5iv / arxiv.org/html）质量更高
  - 其他 PDF 直链 → 下载后走 PDF 流程
  - OpenReview / ACL Anthology HTML 页 → 直接 HTML 解析

## 输出

- `<paper_stem>.zh.md` —— 中文译文（图片引用指向 `<paper_stem>.assets/`）
- `<paper_stem>.assets/` —— 译文专属的图片目录（finalize 阶段从 `assets/` 镜像而来）
- `<paper_stem>.qa.md` —— 质检报告
- `assets/` —— 预处理阶段原始图片（调试用，与 `<paper_stem>.assets/` 内容相同）
- 可选 `<paper_stem>.bilingual.md` —— 双语对照（`--bilingual` 启用）
- 可选 `<paper_stem>.zh.docx` —— Word 文档（`--export-docx` 启用，需 pandoc）

## 执行流程

```
输入 (PDF / URL)
  → fetch.py       下载（URL 情况）
  → preprocess.py  PDF/HTML → 结构化 Markdown (小 PDF 整篇 Marker；大 PDF 分块 Marker；超时/失败回退 pymupdf)
  → segment.py     分段 + 锚点化（锁定公式/代码/图片；表格可锁定或翻译；References 后内容标记排除）
  → translate.py   翻译单元生成 + previous_zh_context + 术语表 + 断点续译
  → postprocess.py 回贴锚点 + 中英排版规范化
  → qa_report.py   阻断级质检 + fix_prompts 返修提示
  → 输出
```

## 使用方式

任意宿主平台（例如 WorkBuddy、OpenClaw、本地脚本编排器）在满足触发条件后，都按如下方式调用。示例中的 `SKILL_DIR` 表示本 skill 所在目录，不要求固定为某个平台的专属路径。

```bash
# 检测 Python
which python3

# 进入 skill 目录，或直接使用脚本绝对路径
export SKILL_DIR=/path/to/technical-paper-translation
cd "$SKILL_DIR"

# 本地 PDF
python3 "$SKILL_DIR/scripts/run.py" \
  --input /path/to/paper.pdf \
  --outdir /path/to/output

# URL 输入
python3 "$SKILL_DIR/scripts/run.py" \
  --input https://arxiv.org/abs/2403.xxxxx \
  --outdir /path/to/output

# 可选参数
--bilingual          同时输出双语对照 Markdown
--export-docx        finalize 阶段额外导出 .docx（需本机安装 pandoc）
--glossary FILE      用户自定义术语表（覆盖内置）
--unit-mode MODE     翻译单元：segment / section / hybrid（默认 hybrid）
--hybrid-max-chars N hybrid 模式下单个翻译单元最大字符数（默认 12000）
--table-mode MODE    表格策略：lock / translate（默认 lock）
--pdf-engine MODE    PDF 解析：auto / marker / pymupdf / marker-chunked（默认 auto）
--marker-timeout N   整篇 Marker 超时时间秒数（默认 900）
--large-pdf-pages N  auto 模式下超过 N 页改用分块 Marker（默认 20）
--pdf-chunk-pages N  分块 Marker 每块页数（默认 12）
--chunk-timeout N    分块 Marker 单块超时时间秒数（默认 300）
--chunk-fallback M   单块失败策略：pymupdf / skip / fail（默认 pymupdf）
--chunk-concurrency N 分块 Marker 并行 worker 数（默认 1；每个 worker 加载 ~1-2GB 模型，建议 2、4）
--retry-fallback     --resume 时，重跑之前 fallback 到 pymupdf/skip/failed 的分块
--table-strategy MODE 表格处理策略：image / markdown（默认 image）
                        image：用 pdfplumber 检测 PDF 中的表格区域并用 pymupdf 截图为 PNG，
                        在 Markdown 中用图片引用替换掉乱的表格文本，译文原样保留图片，
                        翻译 / Word 导出都不会破表。
                        markdown：保留 Marker 抽出的 Markdown 表格，再由 --table-mode 决定锁定或翻译。
--force              跳过阻断级质检（仅在用户明确要求时使用）
--resume             断点续译；复用已有 source.md、segments.json 和已完成 PDF 分块```

## LLM 翻译调用约定（重要）

本 skill 的 `translate.py` 本身不直接调用 LLM API，也不假设运行在某个特定 Agent 产品中。它会把每个翻译单元的 prompt 写入 `prompts_per_segment/*.prompt.md`，由宿主平台或外部 LLM 执行器读取、调用模型，并将译文写回 `zh_per_segment/*.zh.md`。

因此，只要平台具备以下能力即可接入：

1. 执行 `python3 scripts/run.py --stage prepare ...` 生成任务。
2. 读取 `INDEX.md` 和 `prompts_per_segment/*.prompt.md`。
3. 对每个 prompt 调用任意 LLM，并把纯译文写入对应的 `zh_per_segment/<unit_id>.zh.md`。
4. 执行 `python3 scripts/run.py --stage finalize --outdir ...` 组装、回贴锁定块并质检。

如果宿主平台支持 system/user 角色，请把 prompt 文件中的 `# SYSTEM` 用作 system prompt、`# USER` 用作 user message；如果不支持 system 角色，可把 `# SYSTEM` 内容放到 user message 开头。

### wave 并行调度（提速推荐）

`--stage prepare` 阶段除了生成 `prompts_per_segment/` 和 `INDEX.md` 外，还会输出 `waves.json`：

```json
{
  "total_units": 42,
  "num_waves": 8,
  "max_parallel": 9,
  "waves": {
    "0": ["sec_0001", "sec_0002", ...],
    "1": ["sec_0001_part_002", "sec_0002_part_002", ...],
    ...
  }
}
```

- 同一 wave 内的单元彼此**没有 previous_zh 依赖**，可并发调用 LLM。
- 不同 wave 之间必须串行（后面的 wave 需要前一 wave 的 `zh.md` 作为上下文）。
- 外部执行器推荐的伪代码：
  ```python
  waves = json.load(open("waves.json"))["waves"]
  for wave_id in sorted(waves, key=int):
      parallel_run(waves[wave_id], concurrency=N)  # wave 内并行
  ```
- 如宿主不支持并发，按 INDEX.md 的 unit_id 顺序串行调用同样正确。

### PDF 预处理并行

对于大 PDF（页数 > `--large-pdf-pages`，默认 20）自动进入分块 Marker 模式。设置 `--chunk-concurrency 2` 可在内存充足的机器上将预处理时间减少约 40%~50%；每个 worker 会独立加载 Marker 模型（16GB RAM 机器推荐 2，32GB 可试 3~4）。

每个分块在 `preprocess_chunks/<chunk_id>/status.json` 记录使用的引擎（marker / pymupdf / skip / failed）；配合 `--resume --retry-fallback` 可仅重跑之前 fallback 到 pymupdf 的分块，已用 Marker 成功的分块不会被触发。

## 依赖

- Python 3.10+（推荐系统已有的 3.12）
- 首次运行时按需 `pip install -r requirements.txt`
- Marker 会在首次 PDF 解析时下载 ~1–2GB 模型权重
- **可选**：`--export-docx` 依赖系统 pandoc（macOS：`brew install pandoc`；Ubuntu：`apt install pandoc`）

## 版本

v0.2.6（2026-04-26）—— 新增“表格即图片”策略（`--table-strategy image`，默认开启）：使用 pdfplumber 检测 PDF 中的表格区域，通过 pymupdf 2x 清晰度裁剪为 PNG 并插入译文，同时移除 Marker 输出的（常乱排的） Markdown 表格块，彻底解决复杂表格在 md/docx 中破表的问题；原有 `--table-mode lock/translate` 仅在 `--table-strategy markdown` 时生效；docx 导出直接渲染表格图片，不再依赖 pandoc 对复杂表格的有限支持。

v0.2.5（2026-04-26）—— 修复图片破图问题：修正整篇 Marker 模式下图片链接缺少 `assets/` 前缀导致的渲染失败；finalize 阶段自动将 `assets/` 镜像为 `<stem>.assets/` 并重写译文中的图片路径，确保单独搬运 `<stem>.zh.md` 或导出为 Word 时图片仍可渲染；新增 `--export-docx` 选项，通过 pandoc 导出带图片的 .docx。

v0.2.4（2026-04-26）—— 性能优化：PDF 分块 Marker 支持 `--chunk-concurrency` 并行，单块内 status.json 记录引擎；`--retry-fallback` 可配合 `--resume` 仅重跑 fallback 分块；默认 `--large-pdf-pages=20 --pdf-chunk-pages=12`；翻译阶段新增 `waves.json`，同 wave 内单元无 previous_zh 依赖可安全并发调用 LLM。

v0.2.3（2026-04-26）—— PDF 预处理采用组合策略：小 PDF 整篇 Marker 超时回退，大 PDF 分块 Marker 单块回退，并增强 `--resume` 复用已有预处理/分段产物。

v0.2.2（2026-04-26）—— 将 skill 运行协议平台中立化，移除 WorkBuddy 专属假设，补充 WorkBuddy / OpenClaw / 通用 LLM 执行器接入说明。

v0.2.1（2026-04-26）—— 从 References / Bibliography 开始截断，参考文献及其后内容（如 Appendix）不进入最终译文。

v0.2.0（2026-04-26）—— 支持 hybrid/section 翻译单元、上一单元中文上下文、表格策略、B9/B10 质检与自动修复 prompt。

v0.1.0（2026-04-25）—— 初始版本，v1。
