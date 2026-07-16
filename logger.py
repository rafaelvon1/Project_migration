import logging 


logger = logging.getLogger("etl")
logger.setLevel(logging.INFO)

handler = logging.FileHandler("log.txt")

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

handler.setFormatter(formatter)
logger.addHandler(handler)