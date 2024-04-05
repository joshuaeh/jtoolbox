"""Common declarations"""
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

### Project-level declarations###
# misc declarations
pd.options.display.max_rows = 300
pd.options.display.max_columns = 300
pd.plotting.register_matplotlib_converters()

# random seed
rng = np.random.default_rng(seed=42)

# plot style
# sns.set_style("whitegrid")
sns.set_palette(sns.color_palette("colorblind"))
# sns.set_context("paper", font_scale=1.5, rc={"lines.linewidth": 2.5})
# sns.set_palette("colorblind")
sns.set_style("ticks")
sns.set_style({"xtick.direction": "in","ytick.direction": "in"})
sns.set_style({"xtick.bottom": True, "ytick.left": True})

plt.rc('font', family='sans-serif', size=9)
# plt.rc('font.size', 9)
# plt.rc('xtick', labelsize='small')
# plt.rc('ytick', labelsize='small')

plt.rc("savefig", dpi=1_000, bbox="tight", pad_inches=0.01)