import polars as pl

def transform(raw_data: pl.DataFrame, fl_dir: str) -> pl.DataFrame:
    """Transform the data removing columns and adding new ones.

    Args:
        raw_data (pl.DataFrame): data obtained fro the API
        fl_dir (str): Indicates whether the commercial flight is a departure flight (d) or an arrival flight (a)

    Returns:
        pl.DataFrame: simplified dataframe
    """

    transformed_data = raw_data.clone()
    if fl_dir == 'A':
        time_var = "actualLandingTime" # Actual landing time of an arrival
    elif fl_dir == 'D':
        time_var = "actualOffBlockTime" # The actual time of departure of a flight from Amsterdam Airport Schiphol.
    
    KEEP_COLUMNS = ["id",
                    "flightDirection", # Arrival or Departure
                    "serviceType",
                    "flightName",
                    "scheduleDate", # The date on which the scheduled commercial flight will be operated. Example: 2015-06-17.
                    "scheduleTime", # The time the commercial flight is scheduled to depart or arrive. Example: 15:25:00
                    # "publicFlightState",
                    "aircraftType",
                    time_var,
                    "prefixIATA",
                    "terminal"]
    
    transformed_data = transformed_data.select(KEEP_COLUMNS)
    transformed_data = transformed_data.with_columns(scheduleDateTime = pl.col("scheduleDate") +  " " + pl.col("scheduleTime"))

    transformed_data = transformed_data.rename({time_var: 'actualDateTime'})
    transformed_data = transformed_data.drop(["scheduleDate", "scheduleTime"])

    # select aircraftType

    return transformed_data

def transform_and_validate(data):
    """
    This functions checks the data
    """
