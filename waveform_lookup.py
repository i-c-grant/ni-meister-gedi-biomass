import click
import h5py
import yaml

from nmbim import Waveform, WaveformPlotter, app_utils, filters, algorithms


@click.command()
@click.argument("l1b_path", type=click.Path(exists=True))
@click.argument("l2a_path", type=click.Path(exists=True))
@click.argument("l4a_path", type=click.Path(exists=True))
@click.option("--config", "-c", type=click.Path(exists=True), required=True,
              help="Path to the configuration YAML file.")
def cli(l1b_path, l2a_path, l4a_path, config):
    """
    Load L1B, L2A, and L4A files, process the WaveformCollection, and allow
    the user to query shot numbers to plot.
    """
    # Load configuration
    with open(config, 'r') as config_file:
        full_config = yaml.safe_load(config_file)

    # Get the processor configuration
    processor_config = full_config.get('processing_pipeline', {})
    
    # Replace the 'algorithm' key with the actual algorithm function
    processor_kwargs_dict = processor_config.copy()
    for proc_name, proc_config in processor_kwargs_dict.items():
        alg_name = proc_config['alg_fun']
        alg_fun = getattr(algorithms, alg_name)
        proc_config['alg_fun'] = alg_fun

    # Initialize filters
    filter_config = full_config.get('filters', {})

    # Handle case where spatial or temporal filters are specified without parameters
    if 'spatial' in filter_config and not filter_config['spatial']:
        filter_config.pop('spatial')
        click.echo("Spatial filter removed because no boundary file was supplied.")

    if 'temporal' in filter_config and not filter_config['temporal']:
        filter_config.pop('temporal')
        click.echo("Temporal filter removed because no date range was supplied.")

    my_filters = filters.generate_filters(filter_config)

    # Log applied filters
    applied_filters = [name for name, f in my_filters.items() if f is not None]

    if applied_filters:
        for filter_name in applied_filters:
            click.echo(f"{filter_name.capitalize()} filter applied")
    else:
        click.echo("No filters applied")

    # Update the full configuration with runtime filters
    full_config['filters'] = filter_config

    # Log the updated configuration
    click.echo("Updated configuration:")
    click.echo(yaml.dump(full_config))

    # Define the layers for plotting
    layers = {
        # "Smoothed waveform": {
            # "x": "processed/dp_dz",
            # "color": "black",
            # "type": "line",
        # },
        "Estimated ground return": {
            "x": "processed/ground_return",
            "color": "blue",
            "type": "line",
        },
        "Estimated veg. return": {
            "x": "processed/dp_dz_veg_only",
            "color": "darkgreen",
            "type": "line",
        },
        "Raw returns": {
            "x": "processed/wf_raw_scaled",
            "color": "red",
            "type": "scatter",
        },
    }

    plotter = WaveformPlotter(
        layer_dict=layers, y_lab="Height (m)", x_lab="Waveform returns"
    )

    # y path is constant for all layers
    for layer in layers:
        layers[layer]["y"] = "processed/ht"

    with h5py.File(l1b_path, "r") as l1b, h5py.File(l2a_path, "r") as l2a, h5py.File(l4a_path, "r") as l4a:
        # CLI loop to query shot numbers and plot corresponding waveforms
        while True:
            user_input = click.prompt(
                (
                    "Enter shot number to plot, 'quit' to exit, or 'save' "
                    "to save last plot)"
                ),
                type=str,
            )

            if user_input.lower() == "quit":
                click.echo("Exiting the application.")
                break

            elif user_input.lower() == "save":
                plotter.save()
                click.echo("Plot saved.")

            else:
                try:
                    shot_number = int(user_input)
                    waveform = Waveform(
                        l1b=l1b,
                        l2a=l2a,
                        l4a=l4a,
                        shot_number=shot_number,
                    )

                    app_utils.process_waveforms(waveform, processor_kwargs_dict)

                    # Plot the waveform
                    plotter.plot(waveform)

                except (ValueError, IndexError):
                    click.echo(
                        f"Invalid shot number: {user_input}. "
                        f"Please try again."
                    )


if __name__ == "__main__":
    cli()
