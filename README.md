# Spark Demo

## Summary

This pipeline can be broken down into the following major steps:

1. Read the source files *data/gbr.jsonl* and *data/ofac.jsonl* into spark RDD's.
2. Use *app.standardize_record* to map each line in both input files to a standard datatype. The resulting *app.Record* datatypes will have consistent field names and field datatypes.
3. Create a list of all unique pairs of records. Each unique pair consists of one GBR record and one OFAC record.
4. Use *app.get_all_matches* to identify any matching fields in each pair of records. If a match is found, add the field name and field value to a list of matches for that record pair.
5. Filter out the record pairs with empty lists of matches.
6. Drop any duplicate matches. For example, some alias names are repeated but should only appear once in the final output.
7. Write the output to *data/output.jsonl*. An archived version of this output has already been stored under *data/archived_output.jsonl*.

## Matching Criteria

A record in the GBR list was matched to a record in the OFAC list if one of the following criteria was met:

- GBR name = OFAC name
- GBR name = Any OFAC alias
- Any GBR alias = OFAC name
- Any GBR alias = Any OFAC alias
- Any GBR ID number = Any OFAC ID number

Name/alias matches were done after removing special characters `-`, `.`, `,`, and `'` and converting to all lowercase characters.

## Usage

- Python version = 3.8

- Create a virual environment (optional):

```bash
virtualenv .venv
source .venv/bin/activate
```

- Install the requirements:

```bash
pip install -r requirements.txt
```

- Run the app using python:
```bash
python app.py
```

- Or run the app using spark-submit:

```bash
spark-submit --master local[*] app.py
```
