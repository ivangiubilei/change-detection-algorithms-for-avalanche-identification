import glob
import os

import rasterio
from rasterio.merge import merge
from rasterio.plot import show


def mergeTiff(path_root="./quads", suffix="pre"):
    for name in os.listdir(path_root):
        path = os.path.join(path_root, name)
        if not os.path.isdir(path):
            continue

        # find every file which has _before.tiff at the end
        raster_files = glob.glob(f"{path_root}/{name}/*_{suffix}.tiff")
        print("reading:", f"{path_root}/{name}/*_{suffix}.tiff")

        # print("Found raster files:", raster_files)

        src_files_to_mosaic = [rasterio.open(fp) for fp in raster_files]

        # merge
        mosaic, out_transform = merge(src_files_to_mosaic)

        # copy metadata and update it
        out_meta = src_files_to_mosaic[0].meta.copy()
        out_meta.update(
            {
                "driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": out_transform,
            }
        )

        # write to a new file
        with rasterio.open(
            f"./{path_root}/{name}/{name.lower()}_{suffix}_merged.tiff", "w", **out_meta
        ) as dest:
            print(f"{name.lower()}_{suffix}_merged.tiff mosaic written in {path}")
            dest.write(mosaic)


if __name__ == "__main__":
    mergeTiff(path_root="quads-test/")
    mergeTiff(path_root="quads-test/", suffix="post")
