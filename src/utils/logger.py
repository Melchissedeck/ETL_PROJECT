# src/utils/logger.py

import logging
import logging.handlers
import pathlib
import sys
from datetime import datetime

# On part de la racine du projet (2 niveaux au-dessus de ce fichier)
BASE_DIR = pathlib.Path(__file__).resolve().parents[2]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger configuré pour le projet.
    - affichage dans la console
    - écriture dans un fichier tournant dans /logs
    """
    logger = logging.getLogger(name)

    # éviter de doubler les handlers si on appelle plusieurs fois get_logger
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # format du log
    log_format = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1) Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 2) Handler fichier (rotation 5 fichiers de 1 Mo)
    log_file = LOG_DIR / f"etl_{datetime.utcnow().strftime('%Y%m%d')}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    return logger
