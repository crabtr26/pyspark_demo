import os
import shutil
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pyspark.sql import SparkSession


@dataclass
class Address:
    value: str
    country: Optional[str]
    postal_code: Optional[str]


@dataclass
class Alias:
    value: str
    type_: str


@dataclass
class IDNumber:
    value: str
    comment: str


@dataclass
class Record:
    id_: int
    type_: str
    name: str
    aliases: List[Alias]
    nationality: List[str]
    addresses: List[Address]
    id_numbers: List[IDNumber]
    place_of_birth: Optional[str]
    position: Optional[str]
    reported_dates_of_birth: List[str]


def main():

    spark = SparkSession.builder.appName("sparkDemo").getOrCreate()
    sc = spark.sparkContext

    # Map each line in the jsonl file to a 'Record' type with standard attributes
    gbr = sc.textFile("data/gbr.jsonl").map(standardize_record)
    ofac = sc.textFile("data/ofac.jsonl").map(standardize_record)

    # Create a list of all possible pairs of records
    potential_matches = gbr.cartesian(ofac)

    matches = (
        # Create a list of all matching criteria between each pair of records
        potential_matches.map(get_all_matches)
        # Remove the records with an empty list of matching criteria
        .filter(lambda output_record: output_record["matches"])
        # Remove duplicated matching criteria from the final output
        .map(drop_duplicates)
    )

    # Run the pipeline in parallel using all cores. Save output in multiple chunks.
    matches.saveAsTextFile("matches_multi")

    # Read the output chunks and aggregate into a single output file.
    sc.textFile("matches_multi").coalesce(numPartitions=1).saveAsTextFile("matches_one")

    # Clean up temporary files
    os.rename("matches_one/part-00000", "data/output.jsonl")
    shutil.rmtree("matches_multi")
    shutil.rmtree("matches_one")


def standardize_record(input_string: str) -> Record:

    standard_string = input_string.replace("[null]", "[]").replace("null", "None")
    record_dict = eval(standard_string)

    if "id" in record_dict:
        id_ = int(record_dict["id"])
    else:
        raise ValueError("id not found")

    if "type" in record_dict:
        type_ = str(record_dict["type"])
    else:
        raise ValueError("type not found")

    if "name" in record_dict:
        name = str(record_dict["name"])
    else:
        raise ValueError("name not found")

    if "aliases" in record_dict:
        aliases = standardize_aliases(aliases_list=record_dict["aliases"])
    else:
        aliases = []

    if "nationality" in record_dict:
        nationality = record_dict["nationality"]
    else:
        nationality = []

    if "addresses" in record_dict:
        addresses = standardize_addresses(address_list=record_dict["addresses"])
    else:
        addresses = []

    if "id_numbers" in record_dict:
        id_numbers = standardize_id_numbers(id_numbers_list=record_dict["id_numbers"])
    else:
        id_numbers = []

    if "place_of_birth" in record_dict:
        place_of_birth = str(record_dict["place_of_birth"])
    else:
        place_of_birth = None

    if "position" in record_dict:
        position = str(record_dict["position"])
    else:
        position = None

    if "reported_dates_of_birth" in record_dict:
        reported_dates_of_birth = record_dict["reported_dates_of_birth"]
    else:
        reported_dates_of_birth = []

    standard_record = Record(
        id_=id_,
        type_=type_,
        name=name,
        aliases=aliases,
        nationality=nationality,
        addresses=addresses,
        id_numbers=id_numbers,
        place_of_birth=place_of_birth,
        position=position,
        reported_dates_of_birth=reported_dates_of_birth,
    )

    return standard_record


def standardize_addresses(address_list: List[Dict]) -> List[Address]:

    standard_addresses = []

    for address in address_list:
        if "value" in address:
            value = address["value"]
        else:
            value = None
        if "country" in address:
            country = address["country"]
        else:
            country = None
        if "postal_code" in address:
            postal_code = address["postal_code"]
        else:
            postal_code = None

        standard_address = Address(
            value=value, country=country, postal_code=postal_code
        )
        standard_addresses.append(standard_address)

    return standard_addresses


def standardize_aliases(aliases_list: List[Dict]) -> List[Alias]:

    standard_aliases = []

    for alias in aliases_list:
        if "value" in alias:
            value = alias["value"]
        else:
            value = None
        if "type" in alias:
            type_ = alias["type"]
        else:
            type_ = None

        standard_alias = Alias(type_=type_, value=value)
        standard_aliases.append(standard_alias)

    return standard_aliases


def standardize_id_numbers(id_numbers_list: List[Dict]) -> List[IDNumber]:

    standard_id_numbers = []

    for id_numbers in id_numbers_list:
        if "value" in id_numbers:
            value = id_numbers["value"]
        else:
            value = None
        if "comment" in id_numbers:
            comment = id_numbers["comment"]
        else:
            comment = None

        standard_id_number = IDNumber(comment=comment, value=value)
        standard_id_numbers.append(standard_id_number)

    return standard_id_numbers


def get_all_matches(potential_match: Tuple[Record, Record]) -> Dict:

    gbr_record, ofac_record = potential_match
    name_matches = get_name_matches(potential_match)
    alias_matches = get_alias_matches(potential_match)
    id_matches = get_id_matches(potential_match)
    all_matches = name_matches + alias_matches + id_matches

    output_record = {
        "gbr_id": gbr_record.id_,
        "ofac_id": ofac_record.id_,
        "matches": all_matches,
    }

    return output_record


def get_name_matches(potential_match: Tuple[Record, Record]) -> List[Dict]:

    gbr_record, ofac_record = potential_match
    matches = []

    # Check if the GBR name matches the OFAC name
    if clean_name(gbr_record.name) == clean_name(ofac_record.name):
        match_record = {"gbr_name": gbr_record.name, "ofac_name": ofac_record.name}
        matches.append(match_record)

    # Check if the GBR name matches any of the OFAC aliases
    for alias in ofac_record.aliases:
        if clean_name(gbr_record.name) == clean_name(alias.value):
            match_record = {"gbr_name": gbr_record.name, "ofac_alias": alias.value}
            matches.append(match_record)

    return matches


def get_alias_matches(potential_match: Tuple[Record, Record]) -> List[Dict]:

    gbr_record, ofac_record = potential_match
    matches = []

    for gbr_alias in gbr_record.aliases:

        # Check if any of the GBR aliases match the OFAC name
        if clean_name(gbr_alias.value) == clean_name(ofac_record.name):
            match_record = {"gbr_alias": gbr_alias.value, "ofac_name": ofac_record.name}
            matches.append(match_record)

        # Check if any of the GBR aliases match any of the OFAC aliases
        for ofac_alias in ofac_record.aliases:
            if clean_name(gbr_alias.value) == clean_name(ofac_alias.value):
                match_record = {
                    "gbr_alias": gbr_alias.value,
                    "ofac_alias": ofac_alias.value,
                }
                matches.append(match_record)

    return matches


def get_id_matches(potential_match: Tuple[Record, Record]) -> List[Dict]:

    gbr_record, ofac_record = potential_match
    matches = []

    # Check if any of the GBR id numbers match any of the OFAC id numbers
    for gbr_id in gbr_record.id_numbers:
        for ofac_id in ofac_record.id_numbers:
            if gbr_id.value == ofac_id.value:
                match_record = {
                    "gbr_id_number": gbr_id.value,
                    "gbr_comment": gbr_id.comment,
                    "ofac_id_number": ofac_id.value,
                    "ofac_comment": ofac_id.comment,
                }
                matches.append(match_record)

    return matches


def clean_name(name: str) -> str:

    new_name = (
        name.lower()
        .replace("-", " ")
        .replace(".", "")
        .replace(",", "")
        .replace("'", "")
    )

    return new_name


def drop_duplicates(output_record: Dict) -> Dict:

    new_record = {}
    new_record.update(
        {"gbr_id": output_record["gbr_id"], "ofac_id": output_record["ofac_id"]}
    )
    all_matches = output_record["matches"]
    unique_matches = pd.Series(all_matches).drop_duplicates().to_list()
    new_record["matches"] = unique_matches

    return new_record


if __name__ == "__main__":
    main()
