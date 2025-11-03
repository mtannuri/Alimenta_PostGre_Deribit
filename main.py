#!/usr/bin/env python3
# main.py
# Chama o script único Alimenta_PostGre_Deribit.py

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("main")

def main():
    try:
        # importa a função main do script único
        from Alimenta_PostGre_Deribit import main as alimenta_main
    except Exception as e:
        logger.exception("Erro ao importar Alimenta_PostGre_Deribit: %s", e)
        sys.exit(2)

    try:
        alimenta_main()
    except SystemExit:
        raise
    except Exception as e:
        logger.exception("Erro durante execução de Alimenta_PostGre_Deribit: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
