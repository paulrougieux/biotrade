"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to create statement of data policy, for the web platform landfootprint

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
        "ord": [1, 2],
        "source": ["LAFO", "LAFO"],
        "text_before": [
            "Own calculation performed by the EU. Underlying data obtained from UNITED NATIONS. UN COMTRADE DATABASE: licensed under UN COMTRADE conditions available at ",
            "Own calculation performed by the EU. Underlying data obtained from FAO. FAOSTAT Production and Forestry databases, licensed under ",
        ],
        "link1_target": [
            "https://shop.un.org/databases#Comtrade",
            "https://creativecommons.org/licenses/by-nc-sa/3.0/igo/",
        ],
        "link1_text": [
            "https://shop.un.org/databases#Comtrade",
            "CC-BY-NC-SA 3.0 IGO",
        ],
        "text_between": [
            ", extracted from ",
            ", extracted from ",
        ],
        "link2_target": [
            "https://comtradeplus.un.org/BulkFilesSearch",
            "https://www.fao.org/faostat/en/#home",
        ],
        "link2_text": [
            "https://comtradeplus.un.org/BulkFilesSearch",
            "https://www.fao.org/faostat/en/#home",
        ],
        "text_after": [
            f". Date of Access: {db_update}.",
            f". Date of Access: {db_update}.",
        ],
    }
    df = pd.DataFrame(data=statement_dict)
    save_file(df, "lafo_statement.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
