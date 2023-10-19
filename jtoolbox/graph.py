import textwrap

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

class TickRedrawer(matplotlib.artist.Artist):
    """Artist to redraw ticks.
    To use, add the line `ax.add_artist(TickRedrawer())` when creating the plot."""

    __name__ = "ticks"

    zorder = 10

    @matplotlib.artist.allow_rasterization
    def draw(self, renderer: matplotlib.backend_bases.RendererBase) -> None:
        """Draw the ticks."""
        if not self.get_visible():
            self.stale = False
            return

        renderer.open_group(self.__name__, gid=self.get_gid())

        for axis in (self.axes.xaxis, self.axes.yaxis):
            loc_min, loc_max = axis.get_view_interval()

            for tick in axis.get_major_ticks() + axis.get_minor_ticks():
                if tick.get_visible() and loc_min <= tick.get_loc() <= loc_max:
                    for artist in (tick.tick1line, tick.tick2line):
                        artist.draw(renderer)

        renderer.close_group(self.__name__)
        
def append_to_legend(ax, new_legend_entries):
    """ """
    # get current legend entries
    handles, labels = ax.get_legend_handles_labels()
    
    for entry in new_legend_entries:
        handles.append(entry)
        labels.append(entry.get_label())
    return handles, labels
