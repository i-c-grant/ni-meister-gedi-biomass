import matplotlib.pyplot as plt
from typing import Dict, Optional

from nmbim.Waveform import Waveform

from dataclasses import dataclass


@dataclass
class WaveformPlotter:
    """
    Class for plotting data from a Waveform object.

    A WaveformPlotter holds configuration information for one type
    of plot and uses this configuration to plot data from a
    Waveform object received as input.

    Attributes
    ----------
    layer_dict: Dict[str, Dict[str, str]]
        A dictionary mapping layer names to the paths of the data to plot.

    x_path: str
        The path to the data to plot on the x-axis.

    y_path: str
        The path to the data to plot on the y-axis.
    """

    layer_dict: Dict[str, Dict[str, str]]
    x_lab: Optional[str] = None
    y_lab: Optional[str] = None

    def plot(self, wf: Waveform):
        """Plot the data from the Waveform object and store the figure."""
        # Create a new figure
        self.last_fig, ax = plt.subplots(figsize=(12, 8))

        # Add data layers
        for layer_name, layer_data in self.layer_dict.items():
            x_path, y_path = layer_data["x"], layer_data["y"]
            x_data, y_data = wf.get_data(x_path), wf.get_data(y_path)

            # Format as scatter or line depending on layer type
            layer_type = layer_data.get("type", "scatter")
            if layer_type == "scatter":
                marker = "o"
                linestyle = ""
            elif layer_type == "line":
                linestyle = "-"
                marker = ""

            # Plot the layer data
            ax.plot(
                x_data,
                y_data,
                color=layer_data.get("color", "b"),
                marker=marker,
                linestyle=linestyle,
                markersize=2,
                linewidth=2.5,
                label=layer_name,
                antialiased=True,
            )

        # Add a horizontal dashed line at height = 0
        ax.axhline(0, color="black", linestyle="--", linewidth=1)

        # Set the labels and title
        label_size = 10
        title_size = 12

        ax.legend(fontsize=label_size)

        if self.x_lab:
            ax.set_xlabel(self.x_lab, fontsize=label_size)

        if self.y_lab:
            ax.set_ylabel(self.y_lab, fontsize=label_size)

        # Add a title based on the shot number
        shot_number = wf.get_data("metadata/shot_number")
        ax.set_title(
            f"Waveform processing for GEDI shot {shot_number}",
            fontsize=title_size,
            fontweight="bold",
            loc="left",
        )

        # Add an annotation with the biomass index
        biomass_index = wf.get_data("results/biomass_index")
        ax.annotate(
            f"Biomass index: {biomass_index:.2f}",
            xy=(0.70, 0.05),
            xycoords="axes fraction",
            fontsize=label_size,
            fontweight="bold",
        )

        # Store the last shot number for saving
        self.last_shot = shot_number

        # Show the plot
        plt.show()

    def save(self):
        """Save the current plot to a file."""
        if self.last_fig is not None and self.last_shot is not None:
            self.last_fig.savefig(
                f"waveform_plot_{self.last_shot}.png",
                dpi=600,)
        else:
            print("No plot to save.")
