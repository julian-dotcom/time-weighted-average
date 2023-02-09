# =============================================================================
# IMPORTS
# =============================================================================
import os, boto3
from dotenv import load_dotenv
from pprint import pprint
import datetime as dt

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

    def __init__(self, name):
        self.name = name
        self.now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # =============================================================================
    # MAIN
    # =============================================================================
    def main(self):
        self.get_most_recent_update_n_build_start_str()
        self.get_all_epochs()
        self.determine_window_n_fetch_balances()
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
    def get_all_epochs(self):
        KCE = "#pk = :pk"
        EAN = {"#pk": "event"}
        EAV = {":pk": "epoch"}
        res = self.query_dynamodb(EVENTS, KCE, EAN, EAV)
        self.epochs = [
            {"epoch": str(r["info"]["epoch"]), "timestamp": r["timestamp"]} for r in res
        ]

    # =============================================================================
    # NORMALLY, WINDOW = 8hrs, BUT SOMETIMES NO UPDATE EXISTS IN WINDOW. THEN WE NEED
    # TO INCREASE WINDOW SIZE INCREMENTALLY
    # =============================================================================
    def determine_window_n_fetch_balances(self):
        n = 1
        start_obj = self.dt_str_to_obj(self.start.split("#")[-1])
        dt.timedelta(hours=8 * n)
        while True:
            end = self.determine_end_sort_key(start_obj, n)
            balances = self.fetch_balances_for_window(end)
            n += 1

            if len(balances) > 1:  # Need at least 2 to compute pnl
                break
            if end.split("#")[-1] > self.now_str:  # don't iterate into future
                break
            if len(balances) == 0:
                raise Exception("Should always return at least one balance")
        self.balances = balances

    # =============================================================================
    # DETERMINE THE UPPER BOUND SORT KEY FOR QUERYING FOR BALANCES
    # =============================================================================
    def determine_end_sort_key(self, start_obj, n):
        end_obj = start_obj + dt.timedelta(hours=self.TIME_DELTA * n)
        end_str = self.dt_obj_to_str(end_obj)
        epoch = max([e["epoch"] for e in self.epochs if e["timestamp"] < end_str])
        return f"{'0' * (self.EPOCH_N - len(epoch)) + epoch}#{end_str}"

    # =============================================================================
    # FETCH PERIODS FOR SPECIFIC TIME PERIOD
    # =============================================================================
    def fetch_balances_for_window(self, end):
        print(self.start, end, "\n")
        KCE = "#pk = :pk AND #sk BETWEEN :start AND :end"
        EAN = {"#pk": "name", "#sk": "epoch#timestamp"}
        EAV = {":pk": self.name, ":start": self.start, ":end": end}
        bals = self.query_dynamodb(BALANCES_TABLE, KCE, EAN, EAV)
        return self.clean_balances_from_db(bals)

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

    # =============================================================================
    # CONVERT DATETIME OBJECT TO STR
    # =============================================================================
    def dt_obj_to_str(self, dt_obj):
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")

    # =============================================================================
    # CONVERT DATETIME STR TO OBJ
    # =============================================================================
    def dt_str_to_obj(self, dt_str):
        return dt.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    obj = TimeWeightedReturns("bevy_fund")
    pnls = []
    obj.main()
