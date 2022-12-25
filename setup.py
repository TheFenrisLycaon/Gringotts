import pandas as pd
from datetime import datetime
import pickle

YEARS = ["2021", "2022"]
MONTHS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]


class Account:
    def __init__(self) -> None:
        self.df: pd.DataFrame
        self.dfs: dict = {}

    def load_data(self, path: str = "./data/input.csv"):
        df = pd.read_csv(path)
        df = df.drop("converted amount", axis=1)
        df = df.drop("currency.1", axis=1)
        df = df.drop("currency", axis=1)
        df = df.drop("account", axis=1)
        df = df.rename(columns={"description": "temp"})
        df[["subcategory", "description"]] = df["temp"].str.split("::", 1, expand=True)
        df = df.drop("temp", axis=1)
        df["amount"] = pd.to_numeric(df["amount"].str.replace(",", ""))
        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
        self.df = df

    def split_dataframes(self):
        """Splits dataframe into monthly dataframes"""
        for y in YEARS:
            self.dfs[y] = {}
            for m in MONTHS:
                mon = []
                for _, row in self.df.iterrows():
                    if f"{y}-{m}" in str(row["date"]):
                        mon.append(dict(row))
                if len(mon):
                    self.dfs[y][m] = pd.DataFrame(mon)

    def save_dataframes(self):
        """Saves the dataframes as pickles and CSVs."""
        for y in self.dfs.keys():
            for m in self.dfs[y].keys():
                self.dfs[y][m].to_csv(f"data/backup/{y}_{m}.csv")
        with open("data/internal.pkl", "wb") as handle:
            pickle.dump(self.dfs, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def print_dataframe(
        self,
        month: str = str(datetime.today().month),
        year: str = str(datetime.today().year),
    ) -> pd.DataFrame:
        print(self.dfs[year][month])


# def main():
#     input_all = Account(path="./data/input.csv")
#     sample_data = Account(path="./data/sample.csv")

#     input_all.split_dataframes()
#     sample_data.split_dataframes()

#     input_all.format_dataframes()
#     sample_data.format_dataframes()

#     input_all.print_dataframe()


# if __name__ == "__main__":
#     main()


def run():
    acc = Account()
    acc.load_data()
    acc.split_dataframes()
    acc.save_dataframes()

run()