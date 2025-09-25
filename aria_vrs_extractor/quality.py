"""Quality flag evaluation framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Protocol


class QualityEvaluator(Protocol):
    def __call__(self, payload: dict) -> bool: ...


@dataclass(slots=True)
class QualityFlagger:
    enabled_flags: Iterable[str]
    evaluators: Dict[str, QualityEvaluator] = field(default_factory=dict)

    def register(self, name: str, evaluator: QualityEvaluator) -> None:
        if name not in self.enabled_flags:
            raise ValueError(f"Quality flag '{name}' is not enabled in the configuration")
        self.evaluators[name] = evaluator

    def evaluate(self, payload: dict) -> List[str]:
        flags: List[str] = []
        for name in self.enabled_flags:
            evaluator = self.evaluators.get(name)
            if evaluator is None:
                continue
            if evaluator(payload):
                flags.append(name)
        return flags


DEFAULT_FLAGGER = QualityFlagger(enabled_flags=[])


__all__ = ["QualityFlagger", "DEFAULT_FLAGGER"]
