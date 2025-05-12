import geopandas as gpd
from pathlib import Path


# Mapping of country folders to specific shapefiles
layer_map = {
    "EmiliaRomagna2023": {
        "area": "Area_rev.shp",
        "landslides": "Emilia_landslides_v2.shp"
    },
    "Lombok2018": {
        "area": "area.shp",
        "landslides": "LS19-08.shp"
    },
    "Michoacan2022": {
        "area": "investigated_area_LS_Michoacan2022.shp",
        "landslides": "Michoacan2022_LS_polygon.shp"
    },
    "Philippines2019": {
        "area": "area_study.shp",
        "landslides": "landslides_dic2019.shp"
    }
}

# Root directory
inventories_root = Path("inventories")

for country, layers in layer_map.items():
    folder = inventories_root / country
    output_gpkg = inventories_root / f"{country}.gpkg"

    for layer_name, shp_file in layers.items():
        shp_path = folder / shp_file
        if not shp_path.exists():
            print(f"Missing file: {shp_path}")
            continue

        gdf = gpd.read_file(shp_path)
        gdf.to_file(output_gpkg, layer=layer_name, driver="GPKG")
        print(f"{country}: Saved '{layer_name}' from '{shp_file}'")
