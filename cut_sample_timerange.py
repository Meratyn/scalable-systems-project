src = r"C:\trading-trends\data\raw\debs2022-gc-trading-day-10-11-21.csv"
dst = r"C:\trading-trends\data\sample\day1_sample.csv"
start_time = "09:00:00.000"
end_time = "17:30:00.000"

count = 0
with open(src, "r", encoding="utf-8", errors="replace") as f_in, \
     open(dst, "w", encoding="utf-8") as f_out:
    for line in f_in:
        if line.startswith("#") or line.startswith("ID,"):
            continue
        parts = line.rstrip("\n").split(",")
        if len(parts) < 27:
            continue
        last = parts[21]
        trading_time = parts[23]
        trading_date = parts[26]
        if last == "" or trading_time == "" or trading_date == "":
            continue
        try:
            if float(last) == 0.0:
                continue
        except ValueError:
            continue
        if not (start_time <= trading_time <= end_time):
            continue
        f_out.write(line)
        count += 1

print(f"Wrote {count} real price events between {start_time} and {end_time}")