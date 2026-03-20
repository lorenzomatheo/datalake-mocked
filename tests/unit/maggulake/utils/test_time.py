from datetime import datetime as real_datetime

import pytz

from maggulake.utils import time as time_utils

FIXED_DATETIME = real_datetime(2024, 1, 2, 3, 4, 5)


class _FixedDatetime(real_datetime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return FIXED_DATETIME
        return tz.localize(FIXED_DATETIME)


def test_agora_em_sao_paulo_returns_datetime_with_saopaulo_timezone(monkeypatch):
    monkeypatch.setattr(time_utils, "datetime", _FixedDatetime)
    timezone = pytz.timezone("America/Sao_Paulo")

    result = time_utils.agora_em_sao_paulo()

    assert result == timezone.localize(FIXED_DATETIME)
    assert result.tzinfo.zone == "America/Sao_Paulo"


def test_agora_em_sao_paulo_str_formats_time(monkeypatch):
    monkeypatch.setattr(time_utils, "datetime", _FixedDatetime)

    result = time_utils.agora_em_sao_paulo_str()

    assert result == "03:04:05"
