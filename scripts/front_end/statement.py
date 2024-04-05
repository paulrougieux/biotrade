"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to create statement of data policy, for the web platform

"""


def main():
    import pandas as pd
    import os
    import time
    from biotrade.faostat import faostat
    from scripts.front_end.functions import (
        save_file,
    )

    data_dir = faostat.data_dir
    info = os.stat(data_dir)
    # Folder modified
    db_update = time.strftime("%d-%m-%Y", time.localtime(info.st_mtime))
    statement_dict = {
        "source": ["PRDFAO", "TRDFAO", "TRDCOM"],
        "text_before": [
            "Data manipulation performed by the EU. Underlying data obtained from FAO. FAOSTAT Production and Forestry databases, licensed under ",
            "FAOSTAT: Data manipulation performed by the EU. Underlying data obtained from FAO. FAOSTAT Trade database, licensed under ",
            "UN COMTRADE: Data manipulation performed by the EU. Underlying data obtained from UNITED NATIONS, UN COMTRADE DATABASE: licensed under UN COMTRADE conditions available at ",
        ],
        "link1_target": [
            "https://creativecommons.org/licenses/by-nc-sa/3.0/igo/",
            "https://creativecommons.org/licenses/by-nc-sa/3.0/igo/",
            "https://shop.un.org/databases#Comtrade",
        ],
        "link1_text": [
            "CC-BY-NC-SA 3.0 IGO",
            "CC-BY-NC-SA 3.0 IGO",
            "https://shop.un.org/databases#Comtrade",
        ],
        "text_between": [
            ". Extracted from: ",
            ". Extracted from: ",
            ", extracted from ",
        ],
        "link2_target": [
            "https://www.fao.org/faostat/en/#home",
            "https://www.fao.org/faostat/en/#home",
            "https://comtradeplus.un.org/BulkFilesSearch",
        ],
        "link2_text": [
            "https://www.fao.org/faostat/en/#home",
            "https://www.fao.org/faostat/en/#home",
            "https://comtradeplus.un.org/BulkFilesSearch",
        ],
        "text_after": [
            f". Date of Access: {db_update}.",
            f". Date of Access: {db_update}.",
            f". Date of Access: {db_update}.",
        ],
    }
    df = pd.DataFrame(data=statement_dict)
    save_file(df, "statement.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
