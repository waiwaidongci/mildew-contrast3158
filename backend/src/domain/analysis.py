"""对巡检记录分组，计算条件增长倍数并保留命中证据。"""

import time
from datetime import date
from fractions import Fraction

from .spec import SPEC

_FIELDS = SPEC["fields"]
_FIELD_KEYS = {field["key"] for field in _FIELDS}
_EXTRA_LIMIT = SPEC["extra_fields"]["max"]
_EXTRA_NAME_LIMIT = SPEC["extra_fields"]["name_max"]
_TOP_LIMIT = 20
_HEARTBEAT_SECONDS = 15

_CONDITION_LABELS = {
    "high_humidity": ("低于高湿切分点", "高湿"),
    "vent_blocked": ("风口未遮挡", "风口遮挡"),
    "maintenance_pending": ("无待结报修", "设施报修待结"),
}
_PATTERN_COLUMNS = (
    ("pattern_id", "模式编号"), ("conditions", "伴随条件"), ("size", "条件数"),
    ("positive_count", "正组命中"), ("negative_count", "负组命中"),
    ("positive_ids", "正组命中单号"), ("negative_ids", "负组命中单号"),
    ("positive_support", "正组原始支持度"), ("negative_support", "负组原始支持度"),
    ("positive_smoothed", "正组平滑支持度"), ("negative_smoothed", "负组平滑支持度"),
    ("growth", "增长倍数"),
)
_EVIDENCE_COLUMNS = (("pattern_ids", "命中模式编号"),) + tuple(
    (field["key"], field["label"]) for field in _FIELDS
)


class SearchAborted(Exception):
    """后台租约已易主，本次计算作废，等待按断点续跑。"""


def _bounded(value, field):
    allowed = (int,) if field["type"] == "int" else (int, float)
    if type(value) not in allowed or not field["min"] <= value <= field["max"]:
        raise ValueError(f"{field['label']}须为 {field['min']} 至 {field['max']} 范围内的有效数值")
    return value


def _extra_keys(row):
    return [key for key in row if key not in _FIELD_KEYS]


def validate_rows(rows):
    """校验导入与计算共用的巡检口径，返回独立记录。"""
    if not isinstance(rows, (list, tuple)) or not rows:
        raise ValueError("请提供至少一条巡检记录")
    validated = []
    seen = set()
    extra_keys = None
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"第 {index} 条巡检记录格式有误")
        record = {}
        for field in _FIELDS:
            key, kind = field["key"], field["type"]
            value = row.get(key)
            try:
                if kind == "string":
                    if not isinstance(value, str) or not 1 <= len(value.strip()) <= field["max_length"]:
                        raise ValueError(f"{field['label']}为空或长度超限")
                    value = value.strip()
                elif kind == "date":
                    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                        raise ValueError
                elif kind == "bool":
                    if type(value) is not bool:
                        raise ValueError(f"{field['label']}须明确填写是或否")
                else:
                    value = _bounded(value, field)
            except ValueError as error:
                detail = "巡检日期须为有效的 YYYY-MM-DD 日期" if kind == "date" else str(error)
                raise ValueError(f"第 {index} 条巡检记录 · {detail}") from None
            record[key] = value
        extras = _extra_keys(row)
        if extra_keys is None:
            if len(extras) > _EXTRA_LIMIT:
                raise ValueError(f"是非项列最多 {_EXTRA_LIMIT} 列")
            for key in extras:
                if not isinstance(key, str) or not 1 <= len(key) <= _EXTRA_NAME_LIMIT or key != key.strip():
                    raise ValueError(f"是非项列名须为 1 至 {_EXTRA_NAME_LIMIT} 个字符且首尾无空格")
            extra_keys = extras
        elif set(extras) != set(extra_keys):
            raise ValueError(f"第 {index} 条巡检记录 · 是非项列与首条记录不一致")
        for key in extras:
            value = row[key]
            if type(value) is not bool:
                raise ValueError(f"第 {index} 条巡检记录 · {key}须明确填写是或否")
            record[key] = value
        inspection_id = record["inspection_id"]
        if inspection_id.casefold() in seen:
            raise ValueError(f"第 {index} 条巡检记录 · 巡检单号「{inspection_id}」重复")
        seen.add(inspection_id.casefold())
        validated.append(record)
    return validated


def _columns(extra_keys):
    """基础条件列在前，自定义是非项列按表内顺序在后。"""
    return list(_CONDITION_LABELS.items()) + [(key, (f"{key}：否", f"{key}：是")) for key in extra_keys]


class _Search:
    """按列序枚举全部条件组合，用命中位图精确计数，并按分支保存断点。"""

    def __init__(self, records, columns, extra_keys, params, checkpoint, resume):
        self.columns = columns
        self.checkpoint = checkpoint
        self.alpha = Fraction(str(params["smoothing"]))
        self.min_growth = Fraction(str(params["min_growth"]))
        self.min_positive = params["min_positive_count"]
        self.min_total = params["min_total_count"]
        self.positive_mask = 0
        masks = [[0, 0] for _column in columns]
        threshold = params["humidity_threshold"]
        for index, row in enumerate(records):
            bit = 1 << index
            if row["mildew_observed"]:
                self.positive_mask |= bit
            values = (row["humidity"] >= threshold, row["vent_blocked"], row["maintenance_pending"],
                      *(row[key] for key in extra_keys))
            for column, value in enumerate(values):
                masks[column][value] |= bit
        self.masks = masks
        self.negative_mask = (1 << len(records)) - 1 - self.positive_mask
        self.positives = self.positive_mask.bit_count()
        self.negatives = len(records) - self.positives
        # 命中单号集合（位图）→ [最短组合, 正组命中, 负组命中]，只保留当前前 20 名及同档候选
        self.classes = {}
        self.cutoff = None
        self.done = set()
        self.nodes = 0
        self.last_ping = time.monotonic()
        if resume:
            self._load(resume)

    def _load(self, resume):
        index_of = {key: column for column, (key, _labels) in enumerate(self.columns)}
        try:
            for item in resume.get("classes", []):
                pattern = tuple((index_of[key], int(value)) for key, value in item["pattern"])
                self.classes[int(item["mask"], 16)] = (pattern, item["positive"], item["negative"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("续跑进度与当前字段口径不一致，请发起新的运行") from None
        self.done = set(resume.get("done", []))
        self._refresh_cutoff()

    def growth(self, positive, negative):
        smoothed_positive = (positive + self.alpha) / (self.positives + 2 * self.alpha)
        smoothed_negative = (negative + self.alpha) / (self.negatives + 2 * self.alpha)
        return smoothed_positive / smoothed_negative

    def _labels(self, pattern):
        return tuple(self.columns[column][1][value] for column, value in pattern)

    def _rep_key(self, pattern):
        # 同一命中集合只留最短组合，条件数相同按条件名定序
        return (len(pattern), self._labels(pattern))

    def _rank_key(self, item):
        _mask, (pattern, positive, negative) = item
        return (-self.growth(positive, negative), -positive, self._rep_key(pattern))

    def _refresh_cutoff(self):
        # 第 20 名的增长倍数与正组命中；名次只由这两项决定门槛，更差的候选可直接略过
        if len(self.classes) < _TOP_LIMIT:
            self.cutoff = None
            return
        ordered = sorted(self.classes.items(), key=self._rank_key)
        self.cutoff = self._rank_key(ordered[_TOP_LIMIT - 1])[:2]
        tied = [item for item in ordered[_TOP_LIMIT:] if self._rank_key(item)[:2] == self.cutoff]
        self.classes = dict(ordered[:_TOP_LIMIT] + tied)

    def _keep(self, pattern, mask, positive, negative):
        class_key = (-self.growth(positive, negative), -positive)
        if self.cutoff is not None and class_key > self.cutoff and mask not in self.classes:
            return
        current = self.classes.get(mask)
        if current is None or self._rep_key(pattern) < self._rep_key(current[0]):
            self.classes[mask] = (pattern, positive, negative)
            if len(self.classes) > 2 * _TOP_LIMIT:
                self._refresh_cutoff()

    def _visit(self, pattern, mask, next_column):
        positive = (mask & self.positive_mask).bit_count()
        negative = (mask & self.negative_mask).bit_count()
        if positive < self.min_positive or positive + negative < self.min_total:
            return
        if self.growth(positive, negative) >= self.min_growth:
            self._keep(pattern, mask, positive, negative)
        for column in range(next_column, len(self.columns)):
            for value in (0, 1):
                child = mask & self.masks[column][value]
                # 空集无后续；子集与父集相同说明该条件冗余，更短的代表已在本支路上
                if child and child != mask:
                    self._visit(pattern + ((column, value),), child, column + 1)
        self.nodes += 1
        if (self.nodes & 1023) == 0 and time.monotonic() - self.last_ping >= _HEARTBEAT_SECONDS:
            self._ping()

    def _ping(self):
        self.last_ping = time.monotonic()
        if self.checkpoint is not None and not self.checkpoint(self.snapshot()):
            raise SearchAborted

    def run(self):
        for column, (key, _labels) in enumerate(self.columns):
            for value in (0, 1):
                branch = f"{key}:{value}"
                if branch in self.done:
                    continue
                mask = self.masks[column][value]
                if mask:
                    self._visit(((column, value),), mask, column + 1)
                self.done.add(branch)
                self._ping()

    def ranked(self):
        return sorted(self.classes.items(), key=self._rank_key)[:_TOP_LIMIT]

    def snapshot(self):
        # classes 始终是当前前 20 名及同档候选，断点只保存它们与已算完的分支
        return {
            "done": sorted(self.done),
            "classes": [{
                "mask": format(mask, "x"),
                "pattern": [[self.columns[column][0], value] for column, value in pattern],
                "positive": positive,
                "negative": negative,
            } for mask, (pattern, positive, negative) in self.classes.items()],
        }


def _table(title, columns, rows):
    return {"title": title, "columns": [{"key": key, "label": label} for key, label in columns], "rows": rows}


def analyze(rows, normalized_params, checkpoint=None, resume=None):
    """对同一批有效巡检计算可重跑的条件组合排名。

    checkpoint 接收断点进度并返回是否继续持有租约；resume 为上次的断点进度。
    """
    records = validate_rows(rows)
    params = {
        field["key"]: _bounded(normalized_params.get(field["key"], field["default"]), field)
        for field in SPEC["params"]
    }
    total = len(records)
    extra_keys = _extra_keys(records[0])
    search = _Search(records, _columns(extra_keys), extra_keys, params, checkpoint, resume)
    positives, negatives = search.positives, search.negatives
    threshold = params["humidity_threshold"]
    patterns = []
    evidence = []
    if positives and negatives:
        search.run()
        ranked = search.ranked()
        order = sorted(range(total),
                       key=lambda index: (records[index]["inspection_date"], records[index]["inspection_id"]))

        def hit_ids(mask):
            return "、".join(records[index]["inspection_id"] for index in order if mask >> index & 1)

        row_hits = [[] for _row in records]
        for rank, (mask, (pattern, positive, negative)) in enumerate(ranked, start=1):
            pattern_id = f"P{rank:02d}"
            growth = search.growth(positive, negative)
            patterns.append({
                "pattern_id": pattern_id,
                "conditions": "且".join(search._labels(pattern)),
                "size": len(pattern),
                "positive_count": positive,
                "negative_count": negative,
                "positive_ids": hit_ids(mask & search.positive_mask),
                "negative_ids": hit_ids(mask & search.negative_mask),
                "positive_support": positive / positives,
                "negative_support": negative / negatives,
                "positive_smoothed": float((positive + search.alpha) / (positives + 2 * search.alpha)),
                "negative_smoothed": float((negative + search.alpha) / (negatives + 2 * search.alpha)),
                "growth": float(growth),
            })
            remaining = mask
            while remaining:
                bit = remaining & -remaining
                row_hits[bit.bit_length() - 1].append(pattern_id)
                remaining ^= bit
        evidence = [{**records[index], "pattern_ids": "、".join(row_hits[index])}
                    for index in order if row_hits[index]]

    if not positives or not negatives:
        missing_group = "见霉斑" if not positives else "未见霉斑"
        headline = f"缺少{missing_group}的巡检记录，暂无法比较两组。"
    elif patterns:
        headline = f"筛出 {len(patterns)} 个伴随模式，{patterns[0]['conditions']}的增长倍数最高。"
    else:
        headline = "本批巡检中没有模式同时达到命中数与增长倍数门槛。"

    return {
        "state": "warning" if not positives or not negatives or patterns else "ok",
        "headline": headline,
        "note": (
            f"纳入 {total} 条巡检。湿度达到 {threshold:g}% 归入高湿，平滑量 {params['smoothing']:g}。"
            f"门槛为正组命中至少 {params['min_positive_count']} 条、两组合计至少 {params['min_total_count']} 条、"
            f"增长至少 {params['min_growth']:g} 倍。组合不限项数，同一列只取一种取值；"
            f"命中单号完全相同的组合只保留条件数最少的一个，榜单按增长倍数排名取前 {_TOP_LIMIT} 个。"
            f"结果反映本批记录的关联，无法据此判定成因。"
        ),
        "metrics": [
            {"label": "见霉斑巡检", "value": positives, "detail": "正组支持度的分母"},
            {"label": "未见霉斑巡检", "value": negatives, "detail": "负组支持度的分母"},
            {"label": "入选模式", "value": len(patterns),
             "detail": f"组合不限项数，按增长倍数排名取前 {_TOP_LIMIT} 个"},
            {"label": "命中巡检", "value": len(evidence), "detail": "至少符合一个入选模式，按巡检单号计数"},
        ],
        "tables": [
            _table("伴随模式排名", _PATTERN_COLUMNS, patterns),
            _table("巡检证据明细", _EVIDENCE_COLUMNS + tuple((key, key) for key in extra_keys), evidence),
        ],
        "charts": [{"title": "增长倍数最高的模式", "kind": "bar", "series": [
            {"label": item["pattern_id"], "value": item["growth"]} for item in patterns[:8]
        ]}] if patterns else [],
        "sections": [{"title": "按模式复核巡检", "text": (
            "排名表列出每个模式的两组命中单号，可直接按单号回查台账。"
            "用模式编号在巡检证据或下载的 CSV 中查找命中记录，同一条巡检可命中多个模式。"
            "按见霉斑列分组，可核对排名表的两组命中数。"
            "复核时查看同库房同期记录、连续湿度读数和设施报修单，再登记检查安排。"
        )}],
    }
