src = r"C:\trading-trends\data\raw\debs2022-gc-trading-day-08-11-21.csv"
dst = r"C:\trading-trends\data\sample\day1_sample.csv"
target_events = 50000

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
        f_out.write(line)
        count += 1
        if count >= target_events:
            break

print(f"Wrote {count} real price events to {dst}")