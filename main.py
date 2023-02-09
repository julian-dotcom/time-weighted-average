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

    def __init__(self, name, start, end):
        self.name = name
        self.end = end

    # =============================================================================
    # MAIN
    # =============================================================================
    def main(self):
        self.determine_relevant_epochs()
        self.fetch_balances_for_window()
        self.fetch_one_balance_before_window()
        self.determine_period_cutoffs()
        self.determine_period_percentage_pnls()
        self.save_pnls_to_db()
        return self.pnls

    # =============================================================================
    # DETERMINE RELEVANT EPOCH, TO PULL BALANCES LATER
    # can be multiple epochs, if epoch start is between self.start & self.end
    # =============================================================================
    def determine_relevant_epochs(self):
        KCE = "#pk = :pk"
        EAN = {"#pk": "event"}
        EAV = {":pk": "epoch"}
        epochs = self.query_dynamodb(EVENTS, KCE, EAN, EAV)
        start_epochs = []
        end_epochs = []

        for epoch in epochs:
            ep = str(epoch["info"]["epoch"])
            # Find largest epoch that started before self.start
            if epoch["timestamp"] < self.start:
                start_epochs.append(ep)
            # Find if if any epoch deadlines between self.start && self.end
            if epoch["timestamp"] > self.start and epoch["timestamp"] < self.end:
                end_epochs.append(ep)
        start_epoch = max(start_epochs) if len(start_epochs) else "0"

        if len(end_epochs) > 0:
            end_epoch = max(end_epochs)
        end_epoch = max(end_epochs) if len(end_epochs) > 0 else None

        self.prepare_balances_sort_key(start_epoch, end_epoch)

    # =============================================================================
    # PREPARE SORT KEYS TO QUERY BALANCES
    # =============================================================================
    def prepare_balances_sort_key(self, start_epoch, end_epoch):
        start = "0" * (self.EPOCH_N - len(start_epoch)) + start_epoch
        self.sort_start = f"{start}#{self.start}"

        if end_epoch is not None:
            end = "0" * (self.EPOCH_N - len(end_epoch)) + end_epoch
            self.sort_end = f"{end}#{self.end}"
        else:
            end = start
            self.sort_end = f"{end}#{self.end}"

    # =============================================================================
    # FETCH PERIODS FOR SPECIFIC TIME PERIOD
    # =============================================================================
    def fetch_balances_for_window(self):
        KCE = "#pk = :pk AND #sk BETWEEN :start AND :end"
        EAN = {"#pk": "name", "#sk": "epoch#timestamp"}
        EAV = {":pk": self.name, ":start": self.sort_start, ":end": self.sort_end}
        bals = self.query_dynamodb(BALANCES_TABLE, KCE, EAN, EAV)
        self.balances = self.clean_balances_from_db(bals)

    # =============================================================================
    # NEED TO KNOW ONE RECORD BEFORE TIME WINDOW AS BASE VALUE
    # =============================================================================
    def fetch_one_balance_before_window(self):
        if len(self.balances) == 0:
            return
        if self.balances[0]["update_type"] == "initiation":
            return
        first_timestamp = min([b["epoch#timestamp"] for b in self.balances])
        KCE = "#pk = :pk AND #sk < :sk"
        EAN = {"#pk": "name", "#sk": "epoch#timestamp"}
        EAV = {":pk": self.name, ":sk": first_timestamp}

        prev = self.query_dynamodb(BALANCES_TABLE, KCE, EAN, EAV, limit=1, sif=False)

        bal = self.clean_balances_from_db(prev)
        self.balances = bal + self.balances

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
    now = dt.datetime.now(dt.timezone.utc)
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end += dt.timedelta(hours=(8 * (now.hour // 8)))
    obj = TimeWeightedReturns("bevy_fund", end=end)
    pnls = obj.main()


# delta = dt.timedelta(hours=8)
# cur = start
# pnls = []
# while cur < end:
#     print(cur)
#     start_w = cur.strftime("%Y-%m-%d %H:%M:%S")
#     end_w = (cur + delta).strftime("%Y-%m-%d %H:%M:%S")
#     obj = TimeWeightedReturns("bevy_fund", start_w, end_w)
#     pnls.extend(obj.main())
#     cur += delta
#     print()

# pnls = pd.DataFrame(pnls)
# pnls["timestamp"] = pd.to_datetime(pnls["timestamp"])
# pnls["pnl"] = pd.to_numeric(pnls["pnl"])
# pnls["cumulative"] = pnls["pnl"].cumsum()
# print(pnls)

# pnls.plot(x="timestamp", y="cumulative", kind="line")

# plt.xlabel("Timestamp")
# plt.ylabel("PNL")
# plt.title("PNL over Time")

# plt.show()
