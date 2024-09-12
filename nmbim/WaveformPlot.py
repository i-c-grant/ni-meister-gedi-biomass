import numpy as np
import matplotlib.pyplot as plt

from nmbim.Waveform import Waveform

class WaveformPlot:
    """
    Class for plotting data from a Waveform object.

    Attributes
    ----------

    wf: Waveform
        The Waveform object containing the data to plot.

    x_path: str
        The path to the data to plot on the x-axis.

    y_path: str
        The path to the data to plot on the y-axis.
    """
    
    def __init__(self,
                 wf: Waveform,
                 x_path: str = "processed/ht",
                 y_path: str = "raw/wf") -> None:
        self.wf = wf
        self.x_path = x_path
        self.y_path = y_path

    def plot(self):
        x = self.wf.get_data(self.x_path)
        y = self.wf.get_data(self.y_path)
        plt.scatter(x, y)
        plt.xlabel(self.x_path)
        plt.ylabel(self.y_path)
        plt.show()

    def segmentation_plot(self):
        # Get data
        y = self.wf.get_data("processed/ht")
        x = self.wf.get_data("processed/dp_dz_veg_only")

        # Get vegetation and ground separation data
        veg_top = self.wf.get_data("processed/veg_ground_sep/veg_top")
        veg_bottom = self.wf.get_data("processed/veg_ground_sep/veg_bottom")
        ground_top = self.wf.get_data("processed/veg_ground_sep/ground_top")
        ground_bottom = self.wf.get_data("processed/veg_ground_sep/ground_bottom")

        # Get ground return data
        ground_return = self.wf.get_data("processed/ground_return")

        # Subtract ground return from waveform
        x = np.maximum(x - ground_return, 0)
        
        # Create an array for colors
        colors = np.full_like(x, "gray", dtype=object)  # Default color is gray

        # Set colors for points between veg_top and veg_bottom (green)
        veg_mask = (y >= veg_bottom) & (y <= veg_top)
        colors[veg_mask] = "green"

        # Set colors for points between ground_top and ground_bottom (red)
        ground_mask = (y >= ground_bottom) & (y <= ground_top)
        colors[ground_mask] = "red"

        # Create scatter plot with color mapping
        plt.scatter(x, y, c=colors, label="Waveform")

        # Plot the ground return (assuming it's on the same y-axis scale)
        plt.plot(ground_return, y, color="blue", label="Ground Return")

        # Set axis labels
        plt.xlabel(self.x_path)
        plt.ylabel(self.y_path)

        # Add a legend to differentiate the ground return and waveform
        plt.legend()

        # Show plot
        plt.show()
