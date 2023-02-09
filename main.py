# =============================================================================
# IMPORTS
# =============================================================================
import sys, os, boto3, pandas as pd, pytz
from dotenv import load_dotenv
from pprint import pprint
import datetime as dt
import matplotlib.pyplot as plt

load_dotenv()

# =============================================================================
# CONSTANTS
# =============================================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)
dynamodb = session.resource("dynamodb", region_name="eu-west-2")
EVENTS = dynamodb.Table("Events")
BALANCES_TABLE = dynamodb.Table("Balances")
TMR_TABLE = dynamodb.Table("TimeWeightedReturns")


# =============================================================================
# IMPORTANT NOTES
# 1. In the accounting frame work, we use epochs to keep track of fees and stuff
# 2. In this class we just don't care about fees, just if deposits are made.
# 3. Periods in this class is anything that is between deposits, or a set timeframe
# =============================================================================


# =============================================================================
# TIME WEIGHTED RETURNS
# =============================================================================
class TimeWeightedReturns:
    EPOCH_N = 5  # len of epoch string in sort key (e.g.: 00002#2023-01-10 00:00:00)
    TIME_DELTA = 8  # the amount of hours we fetch for

    def __init__(self, name, end):
        self.name = name
        self.end_date = end

    # =============================================================================
    # MAIN
    # =============================================================================
    def main(self):
        self.get_most_recent_update_n_build_start_str()
        self.get_cur_epoch_n_build_end_str()
        self.fetch_balances_for_window()
        self.determine_period_cutoffs()
        self.determine_period_percentage_pnls()
        self.save_pnls_to_db()
        return self.pnls

    # =============================================================================
    # QUERY FOR MOST RECENT UPDATE IN TWR TABLE, BUILD START_STR FOR BALANCES TABLE
    # =============================================================================
    def get_most_recent_update_n_build_start_str(self):
        kce, ean, eav = "#n = :v", {"#n": "name"}, {":v": "bevy_fund"}
        limit, sfi = 1, False
        res = self.query_dynamodb(TMR_TABLE, kce, ean, eav, limit, sfi)[0]
        self.start = f"{res['epoch']}#{res['timestamp']}"

    # =============================================================================
    # QUERY FOR MOST RECENT UPDATE IN TWR TABLE, BUILD START_STR FOR BALANCES TABLE
    # =============================================================================
    def get_cur_epoch_n_build_end_str(self):
        KCE = "#pk = :pk"
        EAN = {"#pk": "event"}
        EAV = {":pk": "epoch"}
        res = self.query_dynamodb(EVENTS, KCE, EAN, EAV)
        epochs = [
            str(r["info"]["epoch"]) for r in res if r["timestamp"] < self.end_date
        ]
        epoch = f"{(self.EPOCH_N - len(str(max(epochs))))* '0'}{max(epochs)}"
        self.end = f"{epoch}#{self.end_date}"

    # =============================================================================
    # FETCH PERIODS FOR SPECIFIC TIME PERIOD
    # =============================================================================
    def fetch_balances_for_window(self):
        KCE = "#pk = :pk AND #sk BETWEEN :start AND :end"
        EAN = {"#pk": "name", "#sk": "epoch#timestamp"}
        EAV = {":pk": self.name, ":start": self.start, ":end": self.end}
        bals = self.query_dynamodb(BALANCES_TABLE, KCE, EAN, EAV)
        self.balances = self.clean_balances_from_db(bals)

    # =============================================================================
    # CLEAN RESPONSE FROM DYNAMODB TO ONLY INCLUDE RELEVANT STUFF
    # =============================================================================
    def clean_balances_from_db(self, unprocessed):
        balances = []
        for r in unprocessed:
            deposit = r.get("fees_n_deposits", {}).get("deposit", 0)
            init_bal = r.get("fees_n_deposits", {}).get("init_bal", None)
            balances.append(
                {
                    "balance": r["balance"],
                    "epoch#timestamp": r["epoch#timestamp"],
                    "update_type": r["update_type"],
                    "deposit": deposit,
                    "pre_deposit": init_bal,
                }
            )
        return balances

    # =============================================================================
    # DETERMINE PERIOD CUT-OFFS, IF THERE ARE ANY
    # =============================================================================
    def determine_period_cutoffs(self):
        periods = []
        for i, bal in enumerate(self.balances):
            if i == 0:  # always append first
                periods.append(bal)
            elif bal["deposit"] != 0:  # append if deposit/withdraw
                periods.append(bal)
            elif i == len(self.balances) - 1:  # always append last
                periods.append(bal)
        self.periods = periods

    # =============================================================================
    # DETERMINE PERIOD PNL, IF THERE ARE MULTIPLE PERIODS, ADJUST
    # =============================================================================
    def determine_period_percentage_pnls(self):
        pnls = []
        for start, end in zip(self.periods, self.periods[1:]):
            end_bal = end["pre_deposit"] or end["balance"]
            pnl = (end_bal - start["balance"]) / start["balance"]

            epoch = end["epoch#timestamp"].split("#")[0]
            end_timestamp = end["epoch#timestamp"].split("#")[-1]
            start_timestamp = start["epoch#timestamp"].split("#")[-1]

            pnls.append(
                {
                    "name": self.name,
                    "pnl": pnl,
                    "timestamp": end_timestamp,
                    "period_start": start_timestamp,
                    "epoch": epoch,
                }
            )
        self.pnls = pnls
        pprint(self.pnls)

    # =============================================================================
    # DETERMINE PERIOD PNL, IF THERE ARE MULTIPLE PERIODS, ADJUST
    # =============================================================================
    def save_pnls_to_db(self):
        with TMR_TABLE.batch_writer() as batch:
            for record in self.pnls:
                batch.put_item(Item=record)

    # =============================================================================
    #
    # HELPERS
    #
    # =============================================================================
    # QUERY DYNAMODB
    # =============================================================================
    def query_dynamodb(self, table, kce, ean, eav, limit=None, sif=None):
        query_kwargs = {
            "KeyConditionExpression": kce,
            "ExpressionAttributeNames": ean,
            "ExpressionAttributeValues": eav,
        }
        if limit is not None:
            query_kwargs["Limit"] = limit
        if sif is not None:
            query_kwargs["ScanIndexForward"] = sif

        res = table.query(**query_kwargs)
        return res.get("Items", [])


if __name__ == "__main__":
    start = dt.datetime(2022, 12, 26, 0, 0, 0)
    end = dt.datetime(2023, 1, 10, 0, 0, 0)
    delta = dt.timedelta(hours=8)
    cur = start
    pnls = []
    while cur < end:
        print(cur)
        time = cur.strftime("%Y-%m-%d %H:%M:%S")
        obj = TimeWeightedReturns("bevy_fund", time)
        pnls.extend(obj.main())
        cur += delta
        print()

    pnls = pd.DataFrame(pnls)
    pnls["timestamp"] = pd.to_datetime(pnls["timestamp"])
    pnls["pnl"] = pd.to_numeric(pnls["pnl"])
    pnls["cumulative"] = pnls["pnl"].cumsum()
    print(pnls)
    pnls.plot(x="timestamp", y="cumulative", kind="line")

    plt.xlabel("Timestamp")
    plt.ylabel("PNL")
    plt.title("PNL over Time")

    plt.show()


# THIS IF FOR STANDARD OPERATION WITH CRONTAB
# now = dt.datetime.now(dt.timezone.utc)
# end = now.replace(hour=0, minute=0, second=0, microsecond=0)
# end += dt.timedelta(hours=(8 * (now.hour // 8)))
# end = str(end).split("#")[-1]
# obj = TimeWeightedReturns("bevy_fund", end="2023-02-02")
# pnls = obj.main()
