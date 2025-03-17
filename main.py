import pandas as pd
import matplotlib.pyplot as plt

file_name = "main_data.txt"

#slice range
start_line = 0
end_line = 10000

time_sample = 2500 # us is used as a time unit

def parse_data(file_name):
    columns = ["type", "slot ID", "Shred ID", "time_stamp"]
    data = pd.read_csv(file_name, skiprows=range(0, start_line), sep=":", names=columns,
                        dtype={"type": str, "slot ID": int, "Shred ID": int, "time_stamp": str},
                        nrows=end_line - start_line)
    data["time_stamp"] = pd.to_numeric(data["time_stamp"], errors="coerce")
    data["time_stamp"] = pd.to_datetime(data["time_stamp"], unit="us", utc=True, errors="coerce")
    data["time_stamp"] = data["time_stamp"].dt.round(f"{time_sample}us")
    return data


def extract_block(data):

    return data


def data_process(data, data_type):
    res = {}
    for block in data["slot ID"].unique():
        name = " ".join([str(block),data_type])
        res[name] = {}
        filtered = data.loc[data["slot ID"] == block]
        #print(f"filtered length:{len(filtered)}")
        total = 0
        for t in filtered["time_stamp"].unique():
            #print(filtered.loc[filtered["time_stamp"] == t])
            total += len(filtered.loc[filtered["time_stamp"] == t])
            #print(f"TIMESTAMP::{t}::total={total}::filtered_length_byT:{len(filtered.loc[filtered["time_stamp"] == t])}")
            res[name][t] = total


    return res


def plot_datasets(datasets):
    plt.style.use("dark_background")
    plt.figure(figsize=(12, 6))
    colors = ["cyan", "red", "orange", "magenta", "blue", "yellow", "green"]
    for i, (slot_id, values) in enumerate(datasets.items()):
        times = sorted(values.keys())
        counts = [values[t] for t in times]
        plt.plot(times, counts, linestyle="-",  # Removed 'marker="o"'
                 color=colors[i % len(colors)], label=f"Slot ID {slot_id}", alpha=0.8, linewidth=2)

    plt.xlabel("Timestamp", fontsize=12, color="white")
    plt.ylabel("Count", fontsize=12, color="white")
    plt.xticks(rotation=45, color="white")
    plt.yticks(color="white")
    plt.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.5)

    plt.legend(loc="upper left", fontsize=10, framealpha=0.6)
    plt.show()

def main():

    data = parse_data(file_name)
    data = extract_block(data)
    shreds = data.loc[data["type"] == "SHRED_RX"]
    print("Shreds are separated...")
    repair = data.loc[data["type"] == "REPAIR_RX"]
    print("Repairs are separated...")
    datasets = data_process(shreds, "shred")
    print("Dataset: shreds added")
    datasets.update(data_process(repair, "repair"))
    print("Dataset: repairs added")
    plot_datasets(datasets)
    
    return 0

if __name__ == "__main__":
    main()
