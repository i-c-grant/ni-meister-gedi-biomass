import glob
from typing import Optional
import pandas as pd
import geopandas as gpd
import click
import concurrent.futures
from pathlib import Path

def load_hexgrid(hex_path: str) -> gpd.GeoDataFrame:
    """Load and prep hexagonal grid with spatial index"""
    hex_gdf = gpd.read_file(hex_path, columns=["EMAP_HEX", "geometry"]).rename(columns={"EMAP_HEX": "hex_id"}).astype({"hex_id": "int64"})
    hex_gdf = hex_gdf.to_crs("EPSG:4326")  # Match GEDI data CRS
    hex_gdf.sindex  # Create spatial index if it doesn't exist
    return hex_gdf

def process_file(gpkg_path: str, hex_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Process single GPKG file and return aggregated results"""
    # Load only needed columns
    gedi_gdf = gpd.read_file(
        gpkg_path, 
        columns=["biwf", "geometry"],
        where="biwf IS NOT NULL"  # Skip null values
    )
    
    # Spatial join with hex grid
    joined = gpd.sjoin(
        gedi_gdf, 
        hex_gdf[["hex_id", "geometry"]], 
        how="inner", 
        predicate="within"
    )
    
    # Aggregate per hexagon
    return joined.groupby("hex_id").agg(
        biwf_mean=("biwf", "mean"),
        n_waveforms=("biwf", "count")
    ).reset_index()

@click.command()
@click.option(
    "--gpkg-pattern",
    required=True,
    help="Glob pattern to match input GPKG files (e.g., './input/*.gpkg')"
)
@click.option(
    "--hex-grid",
    required=True,
    type=click.Path(exists=True),
    help="Path to prepared hexgrid GPKG file"
)
@click.option(
    "--output",
    required=True,
    help="Path for output GeoPackage file (.gpkg)"
)
@click.option(
    "--max-files",
    type=int,
    default=None,
    required=False,
    help="Maximum number of files to process (for testing)"
)
def main(gpkg_pattern: str, hex_grid: str, output: str, max_files: Optional[int] = None):
    """Process GEDI GPKG files to calculate BIWF metrics per EMAP hexagon"""
    # Validate hexgrid exists
    if not Path(hex_grid).exists():
        raise click.FileNotFoundError(f"Hexgrid file {hex_grid} not found")

    # Load hexgrid once
    hex_gdf = load_hexgrid(hex_grid)
    
    # Find all GPKG files
    gpkg_files = glob.glob(gpkg_pattern, recursive=True)
    if max_files:
        gpkg_files = gpkg_files[:max_files]

    total_files = len(gpkg_files)
    click.echo(f"🚀 Starting processing of {total_files} GPKG files...")

    # Process files in parallel using process pool
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit all tasks to the executor
        future_to_file = {
            executor.submit(process_file, file, hex_gdf): file 
            for file in gpkg_files
        }
        
        # Collect results as they complete
        results = []
        completed = 0
        errors = 0
        
        click.echo(f"🔄 Progress: 0/{total_files} (0%) | Errors: 0")
        
        for future in concurrent.futures.as_completed(future_to_file):
            completed += 1
            filename = Path(future_to_file[future]).name
            progress_pct = (completed / total_files) * 100
            
            try:
                result = future.result()
                results.append(result)
                click.echo(
                    f"✅ Completed {filename} | "
                    f"Progress: {completed}/{total_files} ({progress_pct:.1f}%) | "
                    f"Errors: {errors}"
                )
            except Exception as exc:
                errors += 1
                click.echo(
                    f"❌ Error in {filename} | "
                    f"Progress: {completed}/{total_files} ({progress_pct:.1f}%) | "
                    f"Error: {str(exc)[:50]}..."
                )

    # Final status summary
    click.echo(
        f"\n📊 Processing complete! | "
        f"Total: {total_files} | "
        f"Success: {len(results)} | "
        f"Errors: {errors} | "
        f"Success rate: {(len(results)/total_files)*100:.1f}%"
    )

    # Combine results
    combined = pd.concat(results)
    
    # Calculate weighted average
    final = (
        combined.groupby("hex_id")
        .apply(lambda x: (x.biwf_mean * x.n_waveforms).sum() / x.n_waveforms.sum())
        .rename("mean_biwf")
        .reset_index()
    )
    
    # Merge with hex geometries
    final_with_geom = final.merge(
        hex_gdf[["hex_id", "geometry"]], 
        on="hex_id", 
        how="left"  # Maintain all processed hexes
    )
    
    # Create and save GeoDataFrame
    gdf = gpd.GeoDataFrame(
        final_with_geom,
        geometry="geometry",
        crs=hex_gdf.crs
    )
    
    # Ensure output path ends with .gpkg
    output_path = Path(output).with_suffix(".gpkg")
    gdf.to_file(output_path, driver="GPKG")
    click.echo(f"Successfully saved geospatial results to {output_path}")

if __name__ == "__main__":
    main()
