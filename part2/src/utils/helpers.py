import logging
import sys

deprecated_coins = [
    "ETH",
    "AE",
    "AGI",
    "ARN",
    "BCC",
    "BCHABC",
    "BCHSV",
    "BCN",
    "BCPT",
    "BOT",
    "BTCB",
    "BTT",
    "CHAT",
    "CLOAK",
    "CMT",
    "COCOS",
    "DAI",
    "DENT",
    "DGD",
    "EASY",
    "EDO",
    "ENG",
    "ERD",
    "FUEL",
    "GNT",
    "HC",
    "HOT",
    "HSR",
    "ICN",
    "INS",
    "KEY",
    "LEND",
    "LUN",
    "MBL",
    "MCO",
    "MFT",
    "MOD",
    "NCASH",
    "NPXS",
    "OST",
    "PAX",
    "PHX",
    "POE",
    "RCN",
    "RPX",
    "SALT",
    "STORM",
    "STRAT",
    "SUB",
    "SUN",
    "TNB",
    "TNT",
    "TRIG",
    "TUSD",
    "VEN",
    "VIBE",
    "WIN",
    "WINGS",
    "WPR",
    "XZC",
    "BEAR",
    "BKRW",
    "BNBBEAR",
    "BNBBULL",
    "BULL",
    "EOSBEAR",
    "EOSBULL",
    "ETHBEAR",
    "ETHBULL",
    "USDS",
    "USDSB",
    "XRPBEAR",
    "XRPBULL",
    "BTCST",
    "BQX",
    "SNGLS",
    "INCHDOWN",
    "AAVEDOWN",
    "ADADOWN",
    "BCHDOWN",
    "BNBDOWN",
    "BTCDOWN",
    "DOTDOWN",
    "EOSDOWN",
    "ETHDOWN",
    "FILDOWN",
    "LINKDOWN",
    "LTCDOWN",
    "USHIDOWN",
    "SXPDOWN",
    "TRXDOWN",
    "UNIDOWN",
    "XLMDOWN",
    "XRPDOWN",
    "XTZDOWN",
    "YFIDOWN",
    "NCHUP",
    "AVEUP",
    "ADAUP",
    "BCHUP",
    "BNBUP",
    "BTCUP",
    "DOTUP",
    "EOSUP",
    "ETHUP",
    "FILUP",
    "INKUP",
    "LTCUP",
    "SUPER",
    "SHIUP",
    "SXPUP",
    "TRXUP",
    "UNIUP",
    "XLMUP",
    "XRPUP",
    "XTZUP",
    "YFIUP",
    "VIA",
    "1INCHDOWN",
    "1INCHUP",
    "AAVEUP",
    "BZRX",
    "KEEP",
    "NANO",
    "NU",
    "SUSHIDOWN",
    "SUSHIUP",
]


def logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    log_handler.setLevel(logging.DEBUG)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    return logger
