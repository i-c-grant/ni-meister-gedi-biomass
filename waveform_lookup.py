import click
import h5py

from nmbim import Waveform, WaveformPlotter, app_utils, processing_pipelines, filters


@click.command()
@click.argument("l1b_path", type=click.Path(exists=True))
@click.argument("l2a_path", type=click.Path(exists=True))
def cli(l1b_path, l2a_path):
    """
    Load L1B and L2A files, process the WaveformCollection, and allow
    the user to query shot numbers to plot.
    """
    # Processing pipeline
    processor_params = processing_pipelines.biwf_pipeline

    # Initialize filters (from app_utils)
    my_filters = filters.define_filters()

    # Define the layers for plotting
    layers = {
        "Smoothed waveform": {
            "x": "processed/dp_dz",
            "color": "black",
            "type": "line",
        },
        "Estimated ground return": {
            "x": "processed/ground_return",
            "color": "blue",
            "type": "line",
        },
        "Estimated veg. return": {
            "x": "processed/dp_dz_veg_only",
            "color": "red",
            "type": "line",
        },
        "Raw returns": {
            "x": "processed/wf_raw_scaled",
            "color": "#00ff2d",
            "type": "scatter",
        },
    }

    plotter = WaveformPlotter(
        layer_dict=layers, y_lab="Height (m)", x_lab="Waveform returns"
    )

    # y path is constant for all layers
    for layer in layers:
        layers[layer]["y"] = "processed/ht"

    with h5py.File(l1b_path, "r") as l1b, h5py.File(l2a_path, "r") as l2a:
        # CLI loop to query shot numbers and plot corresponding waveforms
        while True:
            input = click.prompt(
                (
                    "Enter shot number to plot, 'quit' to exit, or 'save' "
                    "to save last plot)"
                ),
                type=str,
            )

            if input.lower() == "quit":
                click.echo("Exiting the application.")
                break

            elif input.lower() == "save":
                plotter.save()
                click.echo("Plot saved.")

            else:
                shot_num = input
                try:
                    shot_number = int(shot_num)
                    waveform = Waveform(
                        l1b=l1b,
                        l2a=l2a,
                        shot_number=shot_number,
                    )

                    app_utils.process_waveforms(waveform, processor_params)

                    # Plot the waveform
                    plotter.plot(waveform)

                except (ValueError, IndexError):
                    click.echo(
                        f"Invalid shot number: {shot_num}. "
                        f"Please try again."
                    )


if __name__ == "__main__":
    cli()
