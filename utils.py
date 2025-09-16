import os
from datetime import datetime
from typing import Any

import connect_python
import pandas as pd


def bms_id_to_int(bms_id: str) -> int:
    match bms_id:
        case "BMS 1":
            return 1
        case "BMS 2":
            return 2
        case "BMS 3":
            return 3
        case "BMS 4":
            return 4
        case "BMS 5":
            return 5
        case "BMS 6":
            return 6


def bms_id_to_dec(bms_id: str) -> int:
    match bms_id:
        case "BMS 1":
            return 20
        case "BMS 2":
            return 21
        case "BMS 3":
            return 23
        case "BMS 4":
            return 22
        case "BMS 5":
            return 18
        case "BMS 6":
            return 19


def vmsc_id_to_dec(vmsc_id: str) -> int:
    match vmsc_id:
        case "VMSC 1":
            return 0
        case "VMSC 2":
            return 1
        case "VMSC 3":
            return 3


def flatten_dict(d: dict[Any, dict]) -> dict:
    """Flattens a dictionary of dictionaries, discarding the outermost keys."""
    flattened_dict = {}
    for key, value in d.items():
        if isinstance(value, dict):
            for key, value in value.items():
                flattened_dict[key] = value
        else:
            flattened_dict[key] = value
    return flattened_dict


def df_from_scalar_dict(scalar_dict: dict) -> pd.DataFrame:
    return pd.DataFrame({key: [value] for key, value in scalar_dict.items()})


def log_file_path(parent_dir, name: str, timestamp: datetime | None = None) -> str:
    if timestamp is None:
        timestamp = datetime.now()

    ts_str = timestamp.strftime("%Y%m%d%H%M%S")

    return os.path.join(parent_dir, f"{ts_str}_{name}.csv")


def get_streaming_enabled(client: connect_python.Client) -> bool:
    return bool(client.get_value("streamingEnabled"))


# TODO: Refactor this so it's not a kitchen sink
def write_data(
    path: str,
    client: connect_python.Client,
    df: pd.DataFrame,
    streaming_enabled: bool,
    stream_id: str,
) -> None:
    write_to_file(path, df)
    if streaming_enabled:
        write_to_stream(client, df, stream_id)


def write_to_file(path: str, df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, mode="w", header=True, index=False)


def write_to_stream(client: connect_python.Client, df: pd.DataFrame, stream_id: str) -> None:
    # TODO: Should get timestamps from the dataframe, but datetime.now() is close enough for practical purposes
    timestamp = datetime.now()

    names = []
    values = []
    for channel_name, value in zip(df.columns, df.values[0]):
        if channel_name == "timestamp":
            # Skip the timestamp column
            continue

        if not isinstance(value, float) and not isinstance(value, str):
            value = float(value)

        names.append(channel_name)
        values.append(value)

    client.stream(stream_id, timestamp, names=names, values=values)
