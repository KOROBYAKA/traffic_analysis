import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import argparse
import math


def parse_data(file_name:str, time_sample:int, start:int, end:int):
    columns = ["type", "slot ID", "Shred ID", "FEC ID", "FEC data shreds", "FEC set size", "time_stamp"]
    start = max(start, 1) # skip header
    data = pd.read_csv(file_name, skiprows=range(0, start), sep=":", names=columns,
                        dtype={"type": str, "slot ID": int, "Shred ID": int, "FEC ID":int, "FEC data shreds":int, "FEC set size":int, "time_stamp": str}, nrows=end - start)
    data["time_stamp"] = pd.to_numeric(data["time_stamp"], errors="coerce")
    data["time_stamp"] = pd.to_datetime(data["time_stamp"], unit="us", utc=True, errors="coerce")
    data["time_stamp"] = data["time_stamp"].dt.round(f"{time_sample}us")
    return data

def when_batch_done(data, block_idx:int):
    block_df = data.query(f"`slot ID` == {str(block_idx)}")
    dct = {}
    try:
        batch_sizes = {}
        for tpl in block_df.query("`FEC data shreds` != 0").itertuples():
            batch_sizes[tpl[4]] = tpl[6]

        for id in batch_sizes.keys():
            first_shreds = {}
            uni = []

            for shred in block_df[block_df["FEC ID"] == id].itertuples():
                shred_id = shred[3]
                uni.append(shred_id)
                if shred_id not in first_shreds.keys():
                    first_shreds[shred_id] = shred[7]

            required_shred_id = round(batch_sizes[id]/2)
            time_stamps = list(first_shreds.values())
            endline = f"#ASSEMBLED SHRED#\n" if len(time_stamps) >= required_shred_id else "\n"
            print(f"Batch ID:{id:<4}    Batch size:{batch_sizes[id]:<4}   Required shred amount:{required_shred_id:<4}   Unique shreds received:{(len(time_stamps)):<3}   Total shreds received:{len(uni):<5}", end=endline)
            if len(time_stamps) >= required_shred_id:
                dct[id] = time_stamps[required_shred_id]
    except:
        print("Some error happened...")
    finally:
        return dct

def ready_indicator(dct, shreds_set):
    indicators = {}
    for id, time_stamp in dct.items():
        indicators[time_stamp] = shreds_set[id][time_stamp]
    return indicators


def extract_block(data, block_idx:int):
    res = {}
    duplicate = {}
    block_df = data.loc[data["slot ID"] == block_idx]
    #print(pd.DataFrame(block_df))
    fec_ids = list(set(block_df["FEC ID"]))
    for id in fec_ids:
        rcv_data = {}
        res[id] = {}
        filtered = block_df.loc[block_df["FEC ID"] == id]
        total = 0
        for t in filtered["time_stamp"].unique():
            duplicates = 0
            for shred in filtered.loc[block_df["time_stamp"] == t].itertuples():
                if shred[3] in rcv_data.keys():
                    duplicates += 1

            total += (len(filtered.loc[block_df["time_stamp"] == t]) - duplicates)
            res[id][t] = total

            for shred in filtered.loc[block_df["time_stamp"] == t].itertuples():
                if shred[3] not in rcv_data.keys():
                    rcv_data[shred[3]] = [[],[],[]]
                rcv_data[shred[3]][0].append(shred[7]) #TIMESTAMP
                rcv_data[shred[3]][1].append(total) #CURRENT TOTAL
                rcv_data[shred[3]][2].append(shred[1]) #RECEIVE METHOD (REPAIR/TURBINE)

        for shred in rcv_data.keys():
            if len(rcv_data[shred][1]) > 1:
                duplicate[("|".join([str(shred),str([rcv_data[shred][2]])[3]]))] = [rcv_data[shred][0], rcv_data[shred][1]]

    return res, duplicate


def data_process(data, data_type):
    res = {}
    for block in data["slot number"].unique():
        name = " ".join([str(block),data_type])
        res[name] = {}
        filtered = data.loc[data["slot number"] == block]
        total = 0
        for t in filtered["time_stamp"].unique():
            total += len(filtered.loc[filtered["time_stamp"] == t])
            res[name][t] = total
    return res

def plot_shreds(ax, shreds_dict, duplicate, ready_indicators):
    ax.clear()
    colors = mpl.color_sequences['Set1']
    max_y = 0

    for i, (fec_set_num, time_data) in enumerate(shreds_dict.items()):
        times = sorted(time_data.keys())  # Get timestamps in order
        counts = [time_data[t] for t in times]  # Get corresponding amounts
        ax.plot(times, counts, color=colors[i % len(colors)], alpha=1, linewidth=2)
        max_y = max(max_y, max(counts))
        shred = ax.annotate(f'{fec_set_num}', xy=(times[-1], counts[-1]),
            rotation=90, xytext=(times[-1], counts[-1]+5),
                    arrowprops=dict(facecolor='white', headwidth=2, headlength=3, width=1),)

    for i, (name,(timestamps, totals)) in enumerate(duplicate.items()):
        duplicates = ax.scatter(timestamps, totals, color='red', alpha=1, s=35, label = "Duplicate")



    shred_done = ax.scatter(ready_indicators.keys(), ready_indicators.values(), color='green', alpha=1, s=80, marker='X', label = "Shred-is-done mark")

    ax.set_xlabel("Timestamp", fontsize=12, color="white")
    ax.set_ylabel("Count", fontsize=12, color="white")
    ax.set_ylim([0,max_y + 10])
    ax.tick_params(axis="x",rotation=45, color="white")
    ax.tick_params(axis="y", color="white")
    ax.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.legend(handles=[shred_done,duplicates])


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
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help = "data file path", type=str)
    parser.add_argument("end_line", help = "last line that script reads", type=int)
    parser.add_argument("--start_line", help = "first line that script reads", type=int, default=0)
    parser.add_argument("--time_sample", help = "time sample in microseconds", type=int, default = int(1000*math.sqrt(2)))
    args = parser.parse_args()
    data = parse_data(args.path, args.time_sample, args.start_line, args.end_line)
    plt.style.use("dark_background")
    fig, axes = plt.subplots(figsize=(12,6))

    #stamps = (data["time_stamp"].unique())
    #print(f"STAMPS:{stamps}")

    block_cursor = Cursor(sorted(pd.unique(data["slot ID"])))
    print("SLOT ID's",pd.unique(data["slot ID"]))
    shreds_set, duplicate = extract_block(data, block_cursor.current())
    done_batches = when_batch_done(data, block_cursor.current())
    ready_indicators = ready_indicator(done_batches, shreds_set)
    plot_shreds(axes, shreds_set, duplicate, ready_indicators)
    def on_press(event):
        if event.key == 'right':
            block_cursor.next()
        elif event.key == "left":
            block_cursor.prev()
        elif event.key == "escape":
            exit()

        shreds_set, duplicate = extract_block(data, block_cursor.current())
        done_batches = when_batch_done(data, block_cursor.current())
        ready_indicators = ready_indicator(done_batches, shreds_set)
        plot_shreds(axes, shreds_set, duplicate, ready_indicators)
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
