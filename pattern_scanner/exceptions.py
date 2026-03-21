from __future__ import annotations


class PatternScannerError(Exception):
    pass


class FetchError(PatternScannerError):
    def __init__(self, reason: str, detail: str = ''):
        super().__init__(f'{reason}{": " + detail if detail else ""}')
        self.reason = reason
        self.detail = detail


class InsufficientDataError(PatternScannerError):
    def __init__(self, symbol_or_msg: str, pattern_id: str = '', required: int = 0, actual: int = 0):
        if pattern_id:
            super().__init__(f'{symbol_or_msg} {pattern_id}: need {required}, got {actual}')
        else:
            super().__init__(symbol_or_msg)
        self.symbol     = symbol_or_msg
        self.pattern_id = pattern_id
        self.required   = required
        self.actual     = actual


class MissingColumnError(PatternScannerError):
    def __init__(self, column: str):
        super().__init__(f'Missing required column: {column}')
        self.column = column


class IndicatorComputeError(PatternScannerError):
    def __init__(self, name: str, reason: str):
        super().__init__(f'Indicator {name} failed: {reason}')
        self.name   = name
        self.reason = reason


class LLMCallError(PatternScannerError):
    def __init__(self, module_or_msg: str, reason: str = '', status_code: int = 0):
        if reason:
            super().__init__(f'LLM [{module_or_msg}] HTTP {status_code}: {reason}')
        else:
            super().__init__(module_or_msg)
        self.module      = module_or_msg
        self.reason      = reason
        self.status_code = status_code


class LLMParseError(PatternScannerError):
    def __init__(self, module_or_msg: str, raw: str = ''):
        if raw:
            super().__init__(f'LLM [{module_or_msg}] parse error: {raw[:100]}')
        else:
            super().__init__(module_or_msg)
        self.module = module_or_msg
        self.raw    = raw


class RegimeDetectorError(PatternScannerError):
    pass
