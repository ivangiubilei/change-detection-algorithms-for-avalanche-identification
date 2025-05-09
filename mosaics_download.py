import json
import os
import urllib
from datetime import datetime
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import requests


def shapefile_to_bbox(
    shapefile_path: Path, target_epsg: int = 4326
) -> Tuple[float, float, float, float]:
    """
    Read a shapefile, reproject to the target CRS, and return its bounding box.

    Returns:
        (minx, miny, maxx, maxy) in the target CRS.
    """
    gdf = gpd.read_file(shapefile_path)
    gdf = gdf.to_crs(epsg=target_epsg)
    minx, miny, maxx, maxy = gdf.total_bounds
    return minx, miny, maxx, maxy


# path where annotations are located
ANNOTATIONS_PATH = Path("../landslides-detection-main/inventories/")
LOMBOK_PATH = ANNOTATIONS_PATH / "Lombok2018"
PHILIPPINES_PATH = ANNOTATIONS_PATH / "Philippines2019"
EMILIA_PATH = ANNOTATIONS_PATH / "EmiliaRomagna2023"
MICHOACAN_PATH = ANNOTATIONS_PATH / "Michoacan2022"

# inventories list
INVENTORIES = [
    {
        "name": "Lombok2018",
        "area": LOMBOK_PATH / "area.shp",
        "dates": [datetime(2018, 8, 5), datetime(2018, 8, 19)],
    },
    {
        "name": "Philippines2019",
        "area": PHILIPPINES_PATH / "area_study.shp",
        "dates": [
            datetime(2019, 10, 16),
            datetime(2019, 10, 29),
            datetime(2019, 10, 31),
            datetime(2019, 12, 15),
        ],
    },
    {
        "name": "Michoacan2022",
        "area": EMILIA_PATH / "Area_rev.shp",
        "dates": [datetime(2022, 9, 19)],
    },
    {
        "name": "EmiliaRomagna2023",
        "area": MICHOACAN_PATH / "investigated_area_LS_Michoacan2022.shp",
        "dates": [datetime(2023, 5, 16), datetime(2023, 5, 17)],
    },
]

with open("./key.txt", "r") as f:
    API_key = f.read()

API_URL = "https://api.planet.com/basemaps/v1/mosaics"
session = requests.Session()
session.auth = (API_key, "")


def get_previous_month(month_str, year_str):
    month = int(month_str)
    year = int(year_str)

    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year

    return f"{prev_month:02}", str(prev_year)


def get_next_month(month_str, year_str):
    month = int(month_str)
    year = int(year_str)

    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year

    return f"{next_month:02}", str(next_year)


def download_basemaps_pre(inventories=INVENTORIES, path="quads-before"):
    for el in inventories:
        dates = el["dates"]

        if len(dates) == 1:
            year = dates[0].year
            month = dates[0].month
            month_before, year_before = get_previous_month(month, year)
        else:
            first_year = dates[0].year
            last_year = dates[-1].year
            first_month = dates[0].month
            last_month = dates[-1].month
            month_before, year_before = get_previous_month(first_month, first_year)

        area_bbox = shapefile_to_bbox(el["area"])
        area_bbox_str = ",".join(map(str, area_bbox))

        for tag, y, m in [("pre", year_before, month_before)]:
            mosaic_name = f"global_monthly_{y}_{m}_mosaic"

            print(f"Looking for mosaic: {mosaic_name}")

            # Get the mosaic ID
            res = session.get(API_URL, params={"name__is": mosaic_name}, stream=True)
            if res.status_code != 200:
                print(f"Failed to find mosaic '{mosaic_name}':", res.status_code)
                continue

            try:
                data = res.json()
                if "mosaics" not in data or not data["mosaics"]:
                    print(f"No mosaics found for '{mosaic_name}'")
                    continue
                mosaic_id = data["mosaics"][0]["id"]
            except Exception as e:
                print("Failed to parse mosaic response:", e)
                continue

            print(f"Using mosaic_id: {mosaic_id}")

            # Now get the quads for the bounding box
            quads_url = f"{API_URL}/{mosaic_id}/quads"
            res = session.get(
                quads_url, params={"bbox": area_bbox_str, "minimal": True}, stream=True
            )
            print(f"Status code for quads ({tag}):", res.status_code)

            try:
                data = res.json()
                items = data.get("items", [])
                if not items:
                    print(f"No quads found for {tag}")
                    continue
            except Exception as e:
                print("Error parsing quads JSON:", e)
                continue

            # Download quads
            for i in items:
                link = i["_links"]["download"]
                name = i["id"] + f"_{tag}.tiff"
                DIR = os.path.join(path, el["name"])
                os.makedirs(DIR, exist_ok=True)
                filename = os.path.join(DIR, name)

                if not os.path.isfile(filename):
                    print(f"Downloading {filename}")
                    urllib.request.urlretrieve(link, filename)


def download_basemaps_post(inventories=INVENTORIES, path="quads-after"):
    for el in inventories:
        dates = el["dates"]

        if len(dates) == 1:
            year = dates[0].year
            month = dates[0].month
            month_after, year_after = get_next_month(month, year)
        else:
            first_year = dates[0].year
            last_year = dates[-1].year
            first_month = dates[0].month
            last_month = dates[-1].month
            month_after, year_after = get_next_month(last_month, last_year)

        area_bbox = shapefile_to_bbox(el["area"])
        area_bbox_str = ",".join(map(str, area_bbox))

        for tag, y, m in [("post", year_after, month_after)]:
            mosaic_name = f"global_monthly_{y}_{m}_mosaic"

            print(f"Looking for mosaic: {mosaic_name}")

            # Get the mosaic ID
            res = session.get(API_URL, params={"name__is": mosaic_name}, stream=True)
            if res.status_code != 200:
                print(f"Failed to find mosaic '{mosaic_name}':", res.status_code)
                continue

            try:
                data = res.json()
                if "mosaics" not in data or not data["mosaics"]:
                    print(f"No mosaics found for '{mosaic_name}'")
                    continue
                mosaic_id = data["mosaics"][0]["id"]
            except Exception as e:
                print("Failed to parse mosaic response:", e)
                continue

            print(f"Using mosaic_id: {mosaic_id}")

            # Now get the quads for the bounding box
            quads_url = f"{API_URL}/{mosaic_id}/quads"
            res = session.get(
                quads_url, params={"bbox": area_bbox_str, "minimal": True}, stream=True
            )
            print(f"Status code for quads ({tag}):", res.status_code)

            try:
                data = res.json()
                items = data.get("items", [])
                if not items:
                    print(f"No quads found for {tag}")
                    continue
            except Exception as e:
                print("Error parsing quads JSON:", e)
                continue

            # Download quads
            for i in items:
                link = i["_links"]["download"]
                name = i["id"] + f"_{tag}.tiff"
                DIR = os.path.join(path, el["name"])
                os.makedirs(DIR, exist_ok=True)
                filename = os.path.join(DIR, name)

                if not os.path.isfile(filename):
                    print(f"Downloading {filename}")
                    urllib.request.urlretrieve(link, filename)


def download_basemaps(inventories=INVENTORIES, path="quads"):
    for el in inventories:
        dates = el["dates"]

        if len(dates) == 1:
            year = dates[0].year
            month = dates[0].month
            month_before, year_before = get_previous_month(month, year)
            month_after, year_after = get_next_month(month, year)
        else:
            first_year = dates[0].year
            last_year = dates[-1].year
            first_month = dates[0].month
            last_month = dates[-1].month
            month_before, year_before = get_previous_month(first_month, first_year)
            month_after, year_after = get_next_month(last_month, last_year)

        area_bbox = shapefile_to_bbox(el["area"])
        area_bbox_str = ",".join(map(str, area_bbox))

        for tag, y, m in [
            ("pre", year_before, month_before),
            ("post", year_after, month_after),
        ]:
            mosaic_name = f"global_monthly_{y}_{m}_mosaic"

            print(f"Looking for mosaic: {mosaic_name}")

            # Get the mosaic ID
            res = session.get(API_URL, params={"name__is": mosaic_name}, stream=True)
            if res.status_code != 200:
                print(f"Failed to find mosaic '{mosaic_name}':", res.status_code)
                continue

            try:
                data = res.json()
                if "mosaics" not in data or not data["mosaics"]:
                    print(f"No mosaics found for '{mosaic_name}'")
                    continue
                mosaic_id = data["mosaics"][0]["id"]
            except Exception as e:
                print("Failed to parse mosaic response:", e)
                continue

            print(f"Using mosaic_id: {mosaic_id}")

            # Now get the quads for the bounding box
            quads_url = f"{API_URL}/{mosaic_id}/quads"
            res = session.get(
                quads_url, params={"bbox": area_bbox_str, "minimal": True}, stream=True
            )
            print(f"Status code for quads ({tag}):", res.status_code)

            try:
                data = res.json()
                items = data.get("items", [])
                if not items:
                    print(f"No quads found for {tag}")
                    continue
            except Exception as e:
                print("Error parsing quads JSON:", e)
                continue

            # Download quads
            for i in items:
                link = i["_links"]["download"]
                name = i["id"] + f"_{tag}.tiff"
                DIR = os.path.join(path, el["name"])
                os.makedirs(DIR, exist_ok=True)
                filename = os.path.join(DIR, name)

                if not os.path.isfile(filename):
                    print(f"Downloading {filename}")
                    urllib.request.urlretrieve(link, filename)


if __name__ == "__main__":
    download_basemaps(path="quads-test")
