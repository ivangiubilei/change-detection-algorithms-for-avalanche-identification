import json
import shutil
import urllib.request
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List, Tuple

import elevation
import geopandas as gpd
import rasterio
import requests
from rasterio.merge import merge
from rasterio.warp import transform_bounds
from tqdm import tqdm

# Constants
DOWNLOAD_DIR = Path("./basemaps")
ANNOTATIONS_PATH = Path("./inventories")
KEY_PATH = Path("./key.txt")

API_URL = "https://api.planet.com/basemaps/v1/mosaics"

# Inventories
INVENTORIES = [
    {
        "name": "Lombok2018",
        "dates": [datetime(2018, 8, 5), datetime(2018, 8, 19)],
    },
    {
        "name": "Philippines2019",
        "dates": [
            datetime(2019, 10, 16),
            datetime(2019, 10, 29),
            datetime(2019, 10, 31),
            datetime(2019, 12, 15),
        ],
    },
    {
        "name": "Michoacan2022",
        "dates": [datetime(2022, 9, 19)],
    },
    {
        "name": "EmiliaRomagna2023",
        "dates": [datetime(2023, 5, 16), datetime(2023, 5, 17)],
    },
]


# Read API key from JSON file
with open(KEY_PATH, "r") as f:
    data = json.load(f)
    API_KEY = data["apiKey"]


session = requests.Session()
session.auth = (API_KEY, "")


def shapefile_to_bbox(inv: Dict, target_epsg: int = 4326) -> Tuple[float, float, float, float]:
    """Return bounding box from 'area' layer of GeoPackage for a given inventory."""
    gpkg_path = ANNOTATIONS_PATH / f"{inv['name']}.gpkg"
    gdf = gpd.read_file(gpkg_path, layer="area")
    gdf = gdf.to_crs(epsg=target_epsg)
    return tuple(gdf.total_bounds)


def get_previous_month(month: int, year: int) -> Tuple[int, int]:
    return (12, year - 1) if month == 1 else (month - 1, year)


def get_next_month(month: int, year: int) -> Tuple[int, int]:
    return (1, year + 1) if month == 12 else (month + 1, year)


def merge_quads(folder: Path, tag: str):
    """Merge all quads in a folder (either 'pre' or 'post') into a single GeoTIFF."""
    input_dir = folder / f"{tag}_quads"
    output_path = folder / f"{tag}_merged.tif"

    if output_path.exists():
        print(f"{output_path.name} already exists, skipping merge.")
        return

    tiff_files = list(input_dir.glob("*.tiff"))

    src_files_to_mosaic = [rasterio.open(fp) for fp in tiff_files]
    mosaic, out_trans = merge(src_files_to_mosaic)

    out_meta = src_files_to_mosaic[0].meta.copy()
    out_meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
            "compress": "lzw",
        }
    )

    with rasterio.open(output_path, "w", **out_meta, BIGTIFF='YES') as dest:
        dest.write(mosaic)

    for src in src_files_to_mosaic:
        src.close()

    print(f"Saved merged file to {output_path}")


def download_basemaps(inventories: List[Dict], download_path: Path):
    for inv in inventories:
        dates = inv["dates"]
        name = inv["name"]

        month, year = dates[0].month, dates[0].year
        month_before, year_before = get_previous_month(month, year)
        month_after, year_after = get_next_month(dates[-1].month, dates[-1].year)

        area_bbox = shapefile_to_bbox(inv)
        bbox_str = ",".join(map(str, area_bbox))

        for tag, y, m in [
            ("pre", year_before, month_before),
            ("post", year_after, month_after),
        ]:
            mosaic_name = f"global_monthly_{y}_{m:02}_mosaic"
            tqdm.write(f"[{name}] Downloading {tag}-event area for {y}-{m:02}")

            try:
                res = session.get(API_URL, params={"name__is": mosaic_name})
                res.raise_for_status()
                data = res.json()
                mosaics = data.get("mosaics", [])
                if not mosaics:
                    print(f"No mosaics found for '{mosaic_name}'")
                    continue
                mosaic_id = mosaics[0]["id"]
            except Exception as e:
                print(f"Error retrieving mosaic: {e}")
                continue

            try:
                quads_url = f"{API_URL}/{mosaic_id}/quads"
                items = []
                params = {"bbox": bbox_str, "minimal": True}
                url = quads_url

                while url:
                    res = session.get(url, params=params)
                    res.raise_for_status()
                    data = res.json()
                    items.extend(data.get("items", []))

                    # Clear params after first request to avoid appending repeatedly
                    params = None

                    # Get the _next link (if any) to continue paging
                    url = data.get("_links", {}).get("_next")

                if not items:
                    print(f"No quads found for {tag}")
                    continue
            except Exception as e:
                print(f"Error retrieving quads: {e}")
                continue

            output_dir = download_path / name / f"{tag}_quads"
            output_dir.mkdir(parents=True, exist_ok=True)

            for quad in tqdm(items, desc=f"{name} - {tag}", leave=True):
                download_link = quad["_links"]["download"]
                filename = output_dir / f"{quad['id']}_{tag}.tiff"
                if not filename.exists():
                    try:
                        with NamedTemporaryFile(delete=False) as tmp_file:
                            urllib.request.urlretrieve(download_link, tmp_file.name)
                            shutil.move(tmp_file.name, filename)
                    except Exception as e:
                        print(f"Download failed for {filename}: {e}")
                        if tmp_file and Path(tmp_file.name).exists():
                            Path(tmp_file.name).unlink()  # Cleanup partial file


def get_bounds_wgs84(raster_path: Path) -> Tuple[float, float, float, float]:
    """Get bounding box in EPSG:4326 (WGS84) from any input raster."""
    with rasterio.open(raster_path) as src:
        bounds_wgs84 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    return bounds_wgs84


def download_dtm(inv: Dict, output_dir: Path):
    """Download and clip DTM for the inventory using the bounding box of pre_merged.tif."""
    name = inv["name"]
    folder = output_dir / name
    pre_tif = folder / "pre_merged.tif"
    dtm_path = folder / "dtm.tif"

    if dtm_path.exists():
        print(f"DTM already exists for {name}, skipping download.")
        return

    bounds = get_bounds_wgs84(pre_tif)

    elevation.clip(bounds=bounds, output=dtm_path.resolve())
    elevation.clean()

    print(f"DTM saved to {dtm_path}")


if __name__ == "__main__":
    download_basemaps(INVENTORIES, DOWNLOAD_DIR)

    for inv in INVENTORIES:
        folder = DOWNLOAD_DIR / inv["name"]
        for tag in ["pre", "post"]:
            merge_quads(folder, tag)

        download_dtm(inv, DOWNLOAD_DIR)
