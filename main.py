import pandas as pd
import matplotlib.pyplot as plt
#TODO: please make this a command-line arg. Preferably, read straight from gzip archive
file_name = "turbine_log.txt"

#TODO these should be CLI args too
#slice range
start_line = 0
end_line = 100000

#TODO: constants should be CAPITALIZED
time_sample = 2500 # us is used as a time unit

def parse_data(file_name):
    columns = ["type", "slot ID", "Shred ID", "FEC ID", "time_stamp"]
    data = pd.read_csv(file_name, skiprows=range(0, start_line), sep=":", names=columns,
                        dtype={"type": str, "slot ID": int, "Shred ID": int, "FEC ID":int, "time_stamp": str},
                        nrows=end_line - start_line)
    data["time_stamp"] = pd.to_numeric(data["time_stamp"], errors="coerce")
    data["time_stamp"] = pd.to_datetime(data["time_stamp"], unit="us", utc=True, errors="coerce")
    data["time_stamp"] = data["time_stamp"].dt.round(f"{time_sample}us")
    return data


def extract_block(data, block_idx:int)->dict:
    res = {}
    block_df = data.loc[data["slot ID"]== block_idx]
    fec_ids = list(set(block_df["FEC ID"]))
    #print(fec_ids)
    #print(f"fec_ids\n{fec_ids}")
    block_df.loc[:, "FEC ID"] = block_df["FEC ID"]
    for id in fec_ids:
        name = id
        res[name] = {}
        filtered = block_df.loc[block_df["FEC ID"] == id]
        #print(filtered, id)
        #print("NAME::",name,":::filtered[time_stamp].unique():::", len(filtered["time_stamp"].unique()))
        total = 0
        for t in filtered["time_stamp"].unique():
            total += len(filtered.loc[block_df["time_stamp"] == t])
            res[name][t] = total
            #print(f"TIMESTAMP::{t}::total={total}::filtered_length_byT:{len(filtered.loc[filtered["time_stamp"] == t])}:::FEC_ID:{id}")

    return res


def data_process(data, data_type):
    res = {}
    for block in data["slot number"].unique():
        name = " ".join([str(block),data_type])
        res[name] = {}
        filtered = data.loc[data["slot number"] == block]
        #print(f"filtered length:{len(filtered)}")
        total = 0
        for t in filtered["time_stamp"].unique():
            #print(filtered.loc[filtered["time_stamp"] == t])
            total += len(filtered.loc[filtered["time_stamp"] == t])
            #print(f"TIMESTAMP::{t}::total={total}::filtered_length_byT:{len(filtered.loc[filtered["time_stamp"] == t])}")
            res[name][t] = total


    return res


#Plot datasets with multiple blocks
def plot_datasets(axes, datasets):
    colors = ["cyan", "red", "orange", "magenta", "blue", "yellow", "green"]
    for i, (slot_id, values) in enumerate(datasets.items()):
        times = sorted(values.keys())
        counts = [values[t] for t in times]
        axes.plot(times, counts, linestyle="-",  # Removed 'marker="o"'
                 color=colors[i % len(colors)], label=f"Slot ID {slot_id}", alpha=0.8, linewidth=2)

    axes.set_xlabel("Timestamp", fontsize=12, color="white")
    #TODO finish conversion
    plt.ylabel("Count", fontsize=12, color="white")
    plt.xticks(rotation=45, color="white")
    plt.yticks(color="white")
    plt.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.5)
    plt.legend(loc="upper left", fontsize=10, framealpha=0.6)


def plot_shreds(ax, shreds_dict):
    ax.clear()
    #TODO: please use a colormap
    colors = ["cyan", "red", "orange", "magenta", "blue", "yellow", "green"]

    for i, (fec_set_num, time_data) in enumerate(shreds_dict.items()):
        times = sorted(time_data.keys())  # Get timestamps in order
        counts = [time_data[t] for t in times]  # Get corresponding amounts

        ax.plot(times, counts, color=colors[i % len(colors)], alpha=0.8, linewidth=2)

        ax.annotate(f'FEC set {fec_set_num}', xy=(times[-1], counts[-1]), rotation=45, xytext=(times[-1], counts[-1]+5),
                    arrowprops=dict(facecolor='white', shrink=0.5),
                    )

    ax.set_xlabel("Timestamp", fontsize=12, color="white")
    ax.set_ylabel("Count", fontsize=12, color="white")
    ax.set_ylim([0,80])
    ax.tick_params(axis="x",rotation=45, color="white")
    ax.tick_params(axis="y", color="white")
    ax.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.legend(loc="upper left", fontsize=10, framealpha=0.6)


class Cursor:
    def __init__(self, data):
        self.data=data
        self.index = 0

    def next(self):
        if self.index < len(self.data)-1:
            self.index += 1
        return self.current()

    def prev(self):
        if self.index >= 1:
            self.index -= 1
        return self.current()

    def current(self):
        return self.data[self.index]

def main():
    data = parse_data(file_name)
    plt.style.use("dark_background")
    fig, axes = plt.subplots(figsize=(12,6))

    #stamps = (data["time_stamp"].unique())
    #print(f"STAMPS:{stamps}")

    block_cursor = Cursor( sorted(pd.unique(data["slot ID"])))
    shreds_set = extract_block(data, block_cursor.current())
    plot_shreds(axes, shreds_set)
    def on_press(event):
        if event.key == 'right':
            block_cursor.next()
        elif event.key == "left":
            block_cursor.prev()
        elif event.key == "escape":
            exit()

        shreds_set = extract_block(data, block_cursor.current())
        plot_shreds(axes, shreds_set)
        fig.suptitle(f"Block number {block_cursor.current()}")
        fig.canvas.draw()

    fig.canvas.mpl_connect('key_press_event', on_press)
    plt.show()
    #shreds = data.loc[data["type"] == "SHRED_RX"]
    #stamps = (data["time_stamp"].unique())
    #print(f"STAMPS:{stamps}")
    #print("Shreds are separated...")
    #repair = data.loc[data["type"] == "REPAIR_RX"]
    #print("Repairs are separated...")
    #datasets = data_process(shreds, "shred")
    #print("Dataset: shreds added")
    #datasets.update(data_process(repair, "repair"))
    #print("Dataset: repairs added")
    #plot_datasets(datasets)


    return 0

if __name__ == "__main__":

    main()
