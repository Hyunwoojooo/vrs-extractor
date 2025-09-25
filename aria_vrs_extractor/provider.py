"""Wrapper utilities around projectaria_tools VrsDataProvider."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from _core_pybinds.data_provider import create_vrs_data_provider
from _core_pybinds.sensor_data import SensorDataType, TimeDomain
from _core_pybinds.stream_id import StreamId


@dataclass(frozen=True, slots=True)
class StreamInfo:
    stream_id: StreamId
    numeric_name: str
    label: Optional[str]
    sensor_type: SensorDataType

    def __hash__(self) -> int:
        return hash(self.numeric_name)


class AriaVrsProvider:
    """Convenience wrapper around projectaria_tools.core.data_provider."""

    def __init__(self, vrs_path: str) -> None:
        self.vrs_path = vrs_path
        self.provider = create_vrs_data_provider(vrs_path)
        if not self.provider:
            raise ValueError(f"Unable to open VRS file at {vrs_path}")
        self._streams: List[StreamInfo] = self._scan_streams()

    def _scan_streams(self) -> List[StreamInfo]:
        streams: List[StreamInfo] = []
        for stream_id in self.provider.get_all_streams():
            try:
                label = self.provider.get_label_from_stream_id(stream_id)
            except Exception:  # pragma: no cover - projectaria returns optional
                label = None
            sensor_type = self.provider.get_sensor_data_type(stream_id)
            streams.append(
                StreamInfo(
                    stream_id=stream_id,
                    numeric_name=str(stream_id),
                    label=label,
                    sensor_type=sensor_type,
                )
            )
        return streams

    @property
    def streams(self) -> List[StreamInfo]:
        return list(self._streams)

    def find_streams_by_type(self, sensor_type: SensorDataType) -> List[StreamInfo]:
        return [info for info in self._streams if info.sensor_type == sensor_type]

    def find_stream_by_label(self, *candidates: str) -> Optional[StreamInfo]:
        normalized = [candidate.lower() for candidate in candidates]
        for info in self._streams:
            if not info.label:
                continue
            label_lower = info.label.lower()
            for candidate in normalized:
                if candidate in label_lower:
                    return info
        return None

    def resolve_rgb_stream(self) -> StreamInfo:
        candidates = self.find_stream_by_label("camera-rgb", "rgb")
        if not candidates:
            images = self.find_streams_by_type(SensorDataType.IMAGE)
            if not images:
                raise RuntimeError("No IMAGE streams found in VRS for RGB export")
            return images[0]
        return candidates

    def resolve_et_streams(self) -> Dict[str, StreamInfo]:
        left = self.find_stream_by_label("camera-et-left", "et-left", "eye-left")
        right = self.find_stream_by_label("camera-et-right", "et-right", "eye-right")
        # Aria ET may be single stream representing both eyes; fallback to ANY eye stream
        if not left or not right:
            generic = self.find_stream_by_label("camera-et", "et")
            if generic:
                return {"mono": generic}
        output: Dict[str, StreamInfo] = {}
        if left:
            output["left"] = left
        if right:
            output["right"] = right
        if not output:
            raise RuntimeError("No eye-tracking streams discovered in VRS")
        return output

    def resolve_audio_stream(self) -> StreamInfo:
        audio_streams = self.find_streams_by_type(SensorDataType.AUDIO)
        if not audio_streams:
            raise RuntimeError("No audio streams present in VRS")
        return audio_streams[0]

    def resolve_imu_streams(self) -> List[StreamInfo]:
        streams = self.find_streams_by_type(SensorDataType.IMU)
        if not streams:
            raise RuntimeError("No IMU streams found in VRS")
        return streams

    def resolve_gps_streams(self) -> List[StreamInfo]:
        return self.find_streams_by_type(SensorDataType.GPS)

    def resolve_wifi_streams(self) -> List[StreamInfo]:
        return self.find_streams_by_type(SensorDataType.WPS)

    def resolve_bt_streams(self) -> List[StreamInfo]:
        return self.find_streams_by_type(SensorDataType.BLUETOOTH)

    def deliver_stream(self, stream_ids: Iterable[StreamId]):
        deliver_option = self.provider.get_default_deliver_queued_options()
        deliver_option.deactivate_stream_all()
        for stream_id in stream_ids:
            deliver_option.activate_stream(stream_id)
        return self.provider.deliver_queued_sensor_data(deliver_option)

    def first_timestamp_ns(self, stream_id: StreamId) -> Optional[int]:
        try:
            return self.provider.get_first_time_ns(stream_id, TimeDomain.DEVICE_TIME)
        except Exception:
            return None

    def last_timestamp_ns(self, stream_id: StreamId) -> Optional[int]:
        try:
            return self.provider.get_last_time_ns(stream_id, TimeDomain.DEVICE_TIME)
        except Exception:
            return None


__all__ = ["AriaVrsProvider", "StreamInfo"]
