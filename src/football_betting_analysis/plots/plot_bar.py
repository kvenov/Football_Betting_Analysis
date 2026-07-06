import numpy as np
import matplotlib.pyplot as plt

def plot_bars(features, nrows, ncols, xlabel, ylabel):
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(14, 8))
    axes_flat = axes.flatten()

    num_colors = len(features)
    cmap = plt.get_cmap('viridis')

    colors = [cmap(i) for i in np.linspace(0, 1, num_colors)]
    
    for idx, feature_name in enumerate(features.keys()):
        ax = axes_flat[idx]
        
        plot_data = features[feature_name].dropna()
        
        ax.bar(plot_data.index, plot_data.values, color=colors[idx])
        
        ax.set_title(feature_name, fontsize=12, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        
        ax.set_xticks(plot_data.index)
        
        ax.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    plt.show()
