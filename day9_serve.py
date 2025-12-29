from flows.day9_scheduling_flow import day9_scheduling_flow

if __name__ == "__main__":
    day9_scheduling_flow.serve(
        name="day9-daily",
        cron="45 15 * * *",  # 9:45 AM Chicago (CST) = 15:45 UTC
        tags=["day9", "schedule"],
    )
