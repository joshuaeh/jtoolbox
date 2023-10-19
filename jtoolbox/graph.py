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
        self.stale = False
        
def append_to_legend(ax, new_legend_entries):
    """ """
    # get current legend entries
    handles, labels = ax.get_legend_handles_labels()
    
    for entry in new_legend_entries:
        handles.append(entry)
        labels.append(entry.get_label())
    return handles, labels

def wrap_labels(ax, width, break_long_words=False):
    labels = []
    for label in ax.get_xticklabels():
        text = label.get_text()
        labels.append(textwrap.fill(text, width=width,
                      break_long_words=break_long_words))
    ax.set_xticklabels(labels, rotation=0)
    return

def jitterfy_categorical_scatterplot(ax, aggregation_statistic=None, jitter_width=0.6):
    """Jitterfy a categorical scatterplot"""
    for points in ax.collections:
        vertices = points.get_offsets().data
        if aggregation_statistic is not None:
            group_x_coordinates = np.unique(vertices[:,0])
            for group_x_coordinate in group_x_coordinates:
                group_vertices = vertices[vertices[:, 0] == group_x_coordinate]
                group_aggregate = aggregation_statistic(group_vertices[:, 1])
                ax.hlines(group_aggregate,
                    group_x_coordinate - jitter_width/2,
                    group_x_coordinate + jitter_width/2,
                    color="black", alpha=0.9, linewidth=1,
                    linestyle="--")
        if len(vertices) > 0:
            vertices[:, 0] += np.random.uniform(-jitter_width/2, jitter_width/2, vertices.shape[0])
            points.set_offsets(vertices)
    xticks = ax.get_xticks()
    ax.set_xlim(xticks[0] - jitter_width, xticks[-1] + jitter_width) # the limits need to be moved to show all the jittered dots
    return