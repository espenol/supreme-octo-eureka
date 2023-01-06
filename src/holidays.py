from datetime import date, timedelta

import pandas as pd


def weeknum(dayname):
    if dayname == "Monday":
        return 0
    if dayname == "Tuesday":
        return 1
    if dayname == "Wednesday":
        return 2
    if dayname == "Thursday":
        return 3
    if dayname == "Friday":
        return 4
    if dayname == "Saturday":
        return 5
    if dayname == "Sunday":
        return 6


def alldays(year, whichDayYouWant):
    d = date(year, 1, 1)
    d += timedelta(days=(weeknum(whichDayYouWant) - d.weekday()) % 7)
    while d.year == year:
        yield d
        d += timedelta(days=7)


MIN_YEAR = 2000
MAX_YEAR = 2030

# Get all holidays (from previously downloaded file, Azure opendatasets holidays)
df_holidays = pd.read_parquet("holidays.parquet.gzip")
df_holidays = df_holidays[df_holidays["countryOrRegion"] == "Norway"]
df_holidays = df_holidays[
    (df_holidays["date"] >= f"{MIN_YEAR}-01-01")
    & (df_holidays["date"] < f"{MAX_YEAR}-01-01")
]

# Get all saturdays (sundays are holidays so dont need to get them)
saturdays = []
for year in range(MIN_YEAR, MAX_YEAR):
    saturdays.extend(list(alldays(year, "Saturday")))
saturdays = pd.to_datetime(pd.Series(saturdays, name="date"))  # list to pd.Series
df_saturdays = pd.DataFrame(saturdays)
df_saturdays = df_saturdays.assign(holidayName="Lørdag")

# Combine these bad boys (holidays and saturdays) to get non businessdays
dfs_to_concatenate = (df_saturdays, df_holidays.loc[:, ["date", "holidayName"]])
df = pd.concat(dfs_to_concatenate).sort_values(by=["date"])
df = df.reset_index().drop("index", axis=1)

# groupby yolo
df = df.groupby(by=["date"], as_index=False).agg({"holidayName": ", ".join})

# finally we wish to add the next businessday for each of the dates
# NB: not obvious how to do this so I do a bit more cumbersome process
non_businessdays = list(df["date"])
nextBd = list(df["date"])  # next business day
while True:
    do_break = True
    for idx in range(len(nextBd)):
        if nextBd[idx] in non_businessdays:
            nextBd[idx] = nextBd[idx] + timedelta(days=1)
            do_break = False

    if do_break:
        break

df["nextBusinessDay"]=nextBd

# Save
df = df.rename(columns={"date": "holidayDate"})
df.to_excel("holidays.xlsx")

# Dataframe of duplicates, will return values if the above groubpy statement is removed
# print(df[df.duplicated(['holidayDate'], keep=False)])
