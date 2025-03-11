import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_data():
    file_path = "data.txt"
    columns = ["type", "slot ID", "Shred ID", "time_stamp"]
    data = pd.read_csv(file_path, sep=":", names=columns, dtype={"type": str, "slot ID": int, "Shred ID": int, "time_stamp": str})
    data["time_stamp"] = pd.to_numeric(data["time_stamp"], errors="coerce")
    data["time_stamp"] = pd.to_datetime(data["time_stamp"], unit="us", utc=True, errors="coerce")
    data["time_stamp"] = data["time_stamp"].dt.round("50ms")
    return data


def data_process(data):
    res = {}
    time = data["time_stamp"].unique()
    tmp = {t: 0 for t in time}
    for block in data["slot ID"].unique():
        res[block] = tmp
        filtered = data.loc[data["slot ID"] == block]
        for t in filtered["time_stamp"]:
            res[block][t] = len(filtered.loc[data["time_stamp"] == t])
        print(block)
    return res




def main():

    data = parse_data()
    datasets = data_process(data)




if __name__ == "__main__":
    main()
