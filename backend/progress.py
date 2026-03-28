import json
import os

PROGRESS_FILE = "sc_ingestion_progress.json"


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(year: int, court: str, page: int) -> None:
    data = load_progress()
    if str(year) not in data:
        data[str(year)] = {}
    data[str(year)][court] = page
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_saved_page(year: int, court: str) -> int:
    data = load_progress()
    return data.get(str(year), {}).get(court, 1)


def reset_progress(year: int = None, court: str = None) -> None:
    if year is None:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        return
    data = load_progress()
    if court is None:
        data.pop(str(year), None)
    else:
        if str(year) in data:
            data[str(year)].pop(court, None)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f, indent=2)
