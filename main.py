import pandas as pd
import matplotlib.pyplot as plt

file_name = "turbine_log.txt"

#slice range
start_line = 0
end_line = 100000

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


def extract_block(data):
    res = {}
    block_df = data.loc[data["slot ID"] == data["slot ID"].value_counts().idxmax()]
    fec_ids = list(set(block_df["FEC ID"] // 64))
    #print(fec_ids)
    #print(f"fec_ids\n{fec_ids}")
    block_df.loc[:, "FEC ID"] = (block_df["FEC ID"] // 64).astype(int)
    for id in fec_ids:
        name = f"shred_{id}"
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


#Plot datasets with multiple blocks
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


def plot_shreds(shreds_dict):
    plt.style.use("dark_background")
    plt.figure(figsize=(12, 6))

    colors = ["cyan", "red", "orange", "magenta", "blue", "yellow", "green"]

    for i, (slot_name, time_data) in enumerate(shreds_dict.items()):
        times = sorted(time_data.keys())  # Get timestamps in order
        counts = [time_data[t] for t in times]  # Get corresponding amounts

        plt.plot(times, counts, color=colors[i % len(colors)], label=f"{slot_name}", alpha=0.8, linewidth=2)

    plt.xlabel("Timestamp", fontsize=12, color="white")
    plt.ylabel("Count", fontsize=12, color="white")
    plt.xticks(rotation=45, color="white")
    plt.yticks(color="white")
    plt.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.5)
    plt.legend(loc="upper left", fontsize=10, framealpha=0.6)
    plt.show()




def main():

    data = parse_data(file_name)
    #stamps = (data["time_stamp"].unique())
    #print(f"STAMPS:{stamps}")
    shreds_set = extract_block(data)
    plot_shreds(shreds_set)
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
