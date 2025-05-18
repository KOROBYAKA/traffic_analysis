from http.client import error

import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import argparse



#Parsing data, returns Pandas DataFrame
def parse_data(file_name:str, time_sample:int, start:int, end:int):
    columns = ["type", "slot ID", "Shred ID", "FEC ID", "FEC data shreds", "FEC set size", "time_stamp"]
    start = max(start, 1) # skip header
    data = pd.read_csv(file_name, skiprows=range(0, start), sep=":", names=columns,
                        dtype={"type": str, "slot ID": int, "Shred ID": int, "FEC ID":int, "FEC data shreds":int, "FEC set size":int, "time_stamp": int}, nrows=end - start)
    data["time_stamp"] = pd.to_numeric(data["time_stamp"], errors="coerce")
    data["time_stamp"] = pd.to_datetime(data["time_stamp"], unit="us", utc=True, errors="coerce")
    data["time_stamp"] = data["time_stamp"].dt.round(f"{time_sample}us")
    return data

#Searches when batches for code block were ready to assemble
#Batch done when amount of unique shreds for one FEC Set  >= FEC ID Size/2
#Returns dict {FEC_SET_ID:TIME_STAMP}
def when_batch_done(block_df):
    done_stamps = {}
    try:
        batch_sizes = {}
        for shred in block_df.query("`FEC data shreds` != 0").itertuples():
            batch_sizes[shred[4]] = shred[6]

        for id in batch_sizes.keys():
            first_shreds = {} #Storage for first shreds with same ID inside of batch
            total_shreds_received = 0

            for shred in block_df[block_df["FEC ID"] == id].itertuples():
                shred_id = shred[3]
                total_shreds_received += 1
                if shred_id not in first_shreds.keys():
                    first_shreds[shred_id] = shred[7]

            required_shred_id = round(batch_sizes[id]/2)
            time_stamps = list(first_shreds.values())
            endline = f"#FEC SET COMPLETE#\n" if len(time_stamps) >= required_shred_id else "\n"
            print(f"Batch ID:{id:<4}    Batch size:{batch_sizes[id]:<4}   Required shred amount:{required_shred_id:<4}   Unique shreds received:{(len(time_stamps)):<3}   Total shreds received:{total_shreds_received:<5}", end=endline)
            if len(time_stamps) >= required_shred_id:
                done_stamps[id] = time_stamps[required_shred_id]
    except:
        print("Some error happened...", error)
    finally:
        return done_stamps

#Transforms dict {FEC_SET_ID:TIME_STAMP} into dict format for plotting
#Return dict {TIME_STAMP:TOTAL} as {X:Y} for plotting
def ready_indicator(dct, shreds_set):
    indicators = {}
    for id, time_stamp in dct.items():
        indicators[time_stamp] = shreds_set[id][time_stamp]
    return indicators

#Receives a Pandas DataFrame from function parse_data
#Get a specific block by block's ID
#Return dict duplicates {DUPLICATE_ID:[TIME_STAMPS][TOTALS]}
#Return dict res {{FEC_SET_ID:{TIME_STAMP:TOTAL}}
def extract_block(data, block_df):
    shreds = {}
    duplicate = {}
    #print(pd.DataFrame(block_df))

    for id in block_df["FEC ID"].unique(): #Starting to traverse a FEC Sets
        rcv_data = {} #global storage for all shreds in batch
        shreds[id] = {}
        filtered = block_df.loc[block_df["FEC ID"] == id]
        total = 0

        #Starting to traverse a FEC Set
        for t in filtered["time_stamp"].unique():
            duplicates = 0
            for shred in filtered.loc[block_df["time_stamp"] == t].itertuples():
                if shred[3] in rcv_data.keys():
                    duplicates += 1
                #Calculating amount of duplicates for certain time stamp

            total += (len(filtered.loc[block_df["time_stamp"] == t]) - duplicates)
            shreds[id][t] = total
            for shred in filtered.loc[block_df["time_stamp"] == t].itertuples():
                if shred[3] not in rcv_data.keys():
                    rcv_data[shred[3]] = [[],[],[]]
                rcv_data[shred[3]][0].append(shred[7]) #TIMESTAMP
                rcv_data[shred[3]][1].append(total) #CURRENT TOTAL
                rcv_data[shred[3]][2].append(shred[1]) #RECEIVE METHOD (REPAIR/TURBINE)
                #Storing shred to rcv_data for further duplicate processing

        for shred in rcv_data.keys():
            if len(rcv_data[shred][1]) > 1:
                duplicate[("|".join([str(shred),str([rcv_data[shred][2]])[3]]))] = [rcv_data[shred][0], rcv_data[shred][1]]
                #Creating a structure for duplicates

    return shreds, duplicate

#Depricated, don't delete
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

#Plots graphs
def plot_shreds(ax, shreds_dict, duplicate, ready_indicators):
    ax.clear()
    colors = mpl.color_sequences['Set1']
    max_y = 0

    for i, (fec_set_num, time_data) in enumerate(shreds_dict.items()):
        times = sorted(time_data.keys())  # Get timestamps in order
        counts = [time_data[t] for t in times]  # Get corresponding amounts
        ax.plot(times, counts, color=colors[i % len(colors)], alpha=1, linewidth=2)
        max_y = max(max_y, max(counts))
        shred = ax.annotate(f'{fec_set_num}', xy=(times[-1], counts[-1]), rotation=90, xytext=(times[-1], counts[-1]+5),
            arrowprops=dict(facecolor='white', headwidth=2, headlength=3, width=1),)

    shred_done = ax.scatter(ready_indicators.keys(), ready_indicators.values(), color='green', alpha=1, s=80, marker='X', label = "Batch-is-done mark")
    handles_list = [shred_done]

    if duplicate:
        for i, (name,(timestamps, totals)) in enumerate(duplicate.items()):
            duplicates = ax.scatter(timestamps, totals, color='red', alpha=1, s=35, label = "Duplicate")
        handles_list.append(duplicates)

    ax.set_xlabel("Timestamp", fontsize=12, color="white")
    ax.set_ylabel("Count", fontsize=12, color="white")
    ax.set_ylim([0,max_y + 10])
    ax.tick_params(axis="x",rotation=45, color="white")
    ax.tick_params(axis="y", color="white")
    ax.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.legend(handles=handles_list)

#Tool for changing a block on press
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
    parser.add_argument("--time_sample", help = "time sample in microseconds", type=int, default = int(1))
    args = parser.parse_args()
    data = parse_data(args.path, args.time_sample, args.start_line, args.end_line)
    plt.style.use("dark_background")
    fig, axes = plt.subplots(figsize=(12,6))
    #stamps = (data["time_stamp"].unique())
    #print(f"STAMPS:{stamps}")
    block_cursor = Cursor(sorted(pd.unique(data["slot ID"])))
    print("SLOT ID's:\n",sorted(pd.unique(data["slot ID"])))
    block_print = f"▬▬ι═══════{block_cursor.current()}-═══════ι▬▬"
    print(block_print)

    block_frame = data.loc[data["slot ID"] == block_cursor.current()]
    shreds_set, duplicate = extract_block(data, block_frame)
    done_batches = when_batch_done(block_frame)
    print(block_print)
    ready_indicators = ready_indicator(done_batches, shreds_set)
    plot_shreds(axes, shreds_set, duplicate, ready_indicators)
    def on_press(event):
        if event.key == 'right':
            block_cursor.next()
        elif event.key == "left":
            block_cursor.prev()
        elif event.key == "escape":
            exit()

        block_frame = data.loc[data["slot ID"] == block_cursor.current()]
        block_print = f"▬▬ι═══════{block_cursor.current()}-═══════ι▬▬"
        print(block_print)
        shreds_set, duplicate = extract_block(data, block_frame)
        done_batches = when_batch_done(block_frame)
        print(block_print)
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
