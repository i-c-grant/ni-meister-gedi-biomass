import click
from nmbim import (
    WaveformCollection, WaveformPlot, processing_pipelines, app_utils
)

@click.command()
@click.argument('l1b', type=click.Path(exists=True))
@click.argument('l2a', type=click.Path(exists=True))
def cli(l1b, l2a):
    """
    Load L1B and L2A files, process the WaveformCollection, and allow the user to query shot numbers to plot.
    """
    # Processing pipeline
    processor_params = processing_pipelines.pipeline_test_veg_ground_sep

    # Initialize filters (from app_utils)
    my_filters = app_utils.define_filters()

    # Load the WaveformCollection with caching (but not beamwise)
    click.echo("Loading waveforms...")
    waveforms = WaveformCollection(input_l1b=l1b,
                                   input_l2a=l2a,
                                   cache_beams=True,
                                   filters=my_filters)
    click.echo(f"Loaded {len(waveforms)} waveforms.")

    # Process the waveforms
    click.echo("Processing waveforms...")
    app_utils.process_waveforms(waveforms, processor_params)
    click.echo("Waveforms processed.")

    # CLI loop to query shot numbers and plot corresponding waveforms
    while True:
        shot_num = click.prompt('Enter shot number to plot (or type "quit" to exit)', type=str)
        
        if shot_num.lower() == "quit":
            click.echo("Exiting the application.")
            break

        try:
            # Assuming shot_num is a valid index or can be converted to an integer
            shot_idx = int(shot_num)
            waveform = waveforms.get_waveform(shot_idx)

            # Plot the waveform
            WaveformPlot(waveform).segmentation_plot()

        except (ValueError, IndexError):
            click.echo(f"Invalid shot number: {shot_num}. Please try again.")

if __name__ == "__main__":
    cli()
