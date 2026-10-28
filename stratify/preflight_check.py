import sys
sys.path.insert(0, 'stratify')
sys.path.insert(0, '.')
from config import load_settings
from ingest_data import LumenClient, DateWindow
from stratified_sample import month_bounds_ms

settings = load_settings()
client = LumenClient(settings)

for year, month in [(2024, 7), (2025, 4)]:
    start, end = month_bounds_ms(year, month)
    page = client.search_page(DateWindow(start, end), page=1)
    print(f"{year}-{month:02d}: total_entries={page.total_entries}")