from biotrade.faostat import faostat

doc_string_dict = {
    "cuypers2013_extraction_rates.csv": faostat.coefficients.__class__.cuypers2013.__doc__,
}
for table in doc_string_dict:
    file_path = faostat.config_data_dir / ("readme_" + table[:-3] + "txt")
    with open(file_path, "w") as f:
        f.write(doc_string_dict[table])
