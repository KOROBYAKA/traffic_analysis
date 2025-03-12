import pandas as pd
import matplotlib.pyplot as plt


def parse_data():
    file_path = "data.txt"
    columns = ["type", "slot ID", "Shred ID", "time_stamp"]
    data = pd.read_csv(file_path, sep=":", names=columns, dtype={"type": str, "slot ID": int, "Shred ID": int, "time_stamp": str})
    data["time_stamp"] = pd.to_numeric(data["time_stamp"], errors="coerce")
    data["time_stamp"] = pd.to_datetime(data["time_stamp"], unit="us", utc=True, errors="coerce")
    data["time_stamp"] = data["time_stamp"].dt.round("25ms")

    return data


def data_process(data, data_type):
    res = {}
    for block in data["slot ID"].unique():
        name = " ".join([str(block),data_type])
        res[name] = {}
        filtered = data.loc[data["slot ID"] == block]
        total = 0
        for t in filtered["time_stamp"]:
            total += len(filtered.loc[filtered["time_stamp"] == t])
            res[name][t] = total

    return res


def plot_datasets(datasets):
    plt.style.use("dark_background")  # Dark mode

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

    data = parse_data()
    shreds = data.loc[data["type"] == "SHRED_RX"]
    print("Shreds are separated...")
    repair = data.loc[data["type"] == "REPAIR_RX"]
    print("Repairs are separated...")
    datasets = data_process(shreds, "shred")
    print("Dataset: shreds added")
    datasets.update(data_process(repair, "repair"))
    print("Dataset: repairs added")
    plot_datasets(datasets)


if __name__ == "__main__":
    main()
