import re
from pathlib import Path
from typing import Callable, Optional
import datetime
import typer
import python_baserow_simple
from dateutil.relativedelta import relativedelta
import pandas as pd

from attrs import define, field

from .config import settings


def one(fun):
    def wraps(data):
        if data:
            return fun(data[0])
        return ""
    return wraps


def concat(sep):
    def decorator(fun):
        def wraps(data):
            results = []
            for entry in data:
                result = fun(entry)
                results.append(result)
            merged = sep.join(str(r) for r in results)
            return merged
        return wraps
    return decorator


def nop(data):
    return data


def constant(const_value):
    def fun(_):
        return const_value
    return fun


def fmt_data(fmt_str):
    def fun(data):
        return fmt_str.format_map(data)
    return fun


def age(data):
    birthdate = data[0]
    cur_date = data[1]

    age_years = -1
    age_months = -1

    if birthdate and cur_date:
        birthdate = datetime.date.fromisoformat(birthdate)
        cur_date = datetime.date.fromisoformat(cur_date)
        age_delta = relativedelta(cur_date, birthdate)
        age_years = age_delta.years
        age_months = age_delta.years * 12 + age_delta.months

    return age_months


def fmt_join(sep):
    def fun(data):
        return sep.join(data)
    return fun


def select(key):

    def wrap(func):
        def fun(data):
            value = data.get(key, "")
            return func(value)
        return fun

    return wrap


def regex_once(query, group=0):
    def inner(data):
        if data:
            if m := re.search(query, data):
                return m.group(group)
        return ""
    return inner


def dict_mapping(maps, default: Optional[str]=""):
    def wrapper(fun):
        def inner(data):
            d = default
            if default is None:
                d = data
            value = maps.get(data, d)
            return fun(value)
        return inner
    return wrapper


def clean_hpo(data):
    hpo_terms = []
    if data:
        hpo_terms = re.findall(r"HPO?:\d+", data)
    return ",".join(hpo_terms)


def fmt_date(data):
    if not data:
        return ""
    dt = datetime.date.fromisoformat(str(data))
    return dt.strftime("%d.%m.%Y")


@define
class Mapping:
    destination: str
    source_keys: list[str] = []
    mapper: Callable = one(nop)

    def map(self, data):
        mapped = self.mapper([data[k] for k in self.source_keys])
        return mapped


OUTPUTS_EXOMAG = [
    Mapping("internal case ID", ["Medgen ID"]),
    Mapping("sequencing lab", mapper=constant("Berlin")),
    Mapping("GestaltMatcher ID", mapper=constant("")),
    Mapping("prenatal", mapper=constant("")),
    Mapping("DoB", mapper=constant("")),
    Mapping("age in months", ["Birthdate", "Datum Labor"], mapper=age),
    Mapping("age in years", mapper=constant("")),
    Mapping("sex", ["Gender"]),
    Mapping("referring clinician", ["Clinician"], mapper=one(one(fmt_data("{Title} {Firstname} {Lastname} ({Email})")))),
    Mapping("Start der Diagnostik", ["Datum Labor"], mapper=one(fmt_date)),
    Mapping("Befunddatum", ["Datum Befund"], mapper=one(fmt_date)),
    Mapping("HPO terms", ["HPO Terms"], mapper=one(clean_hpo)),
    Mapping("bisherige Diagnostik", ["Bisherige Diagnostik"], mapper=one(fmt_join(", "))),
    Mapping("single/duo/trio", ["Analysezahl"]),
    Mapping("Selektivvertrag", ["Vertrag"], mapper=one(dict_mapping({"Selektivvertrag": "ja", "Kostenübernahme": "beantragt"}, "nein")(nop))),
    Mapping("disease category", mapper=constant("")),
    Mapping("case solved/unsolved/unclear", ["Case Status"], mapper=one(dict_mapping({"VUS": "unclear"}, default=None)(nop))),
    Mapping("changes in management/therapy after test", mapper=constant("")),
    Mapping("relevant findings for research", mapper=constant("")),
    Mapping("Test conducted", ["Falltyp"]),
    Mapping("wet lab meta info", mapper=constant("")),
    Mapping("AutoCasc", mapper=constant("")),
    Mapping("autozygosity", mapper=constant("")),
    Mapping("gene", ["Findings"], mapper=one(concat("/")(fmt_data("{Genename}")))),
    Mapping("variant_solves_case", ["Findings"],
            mapper=one(concat("/")(select("ResultType")(dict_mapping({"Main": "primary", "Incidental": "incidental", "Research": "candidate"})(nop))))
            ),
    Mapping("if new disease gene, level of evidence", mapper=constant("")),
    Mapping("pmid", mapper=constant("")),
    Mapping("ISCN", mapper=constant("")),
    Mapping("HGVS_gDNA", mapper=constant("")),
    Mapping("HGVS_cDNA", ["Findings"], mapper=
            one(concat("/")(select("Mutation")(regex_once(r"c.[ATCG>+\-_\dA-Za-z]+"))))
            ),
    Mapping("HGVS_protein", ["Findings"], mapper=one(concat("/")(select("Mutation")(regex_once(r"p.[ATCG>+\-_\dA-Za-z*\(\)]+"))))),
    Mapping("ACMG class", ["Findings"], mapper=one(concat("/")(select("ACMG Classification")(nop)))),
    Mapping("zygosity", ["Findings"], mapper=one(concat("/")(select("Zygosity")(nop)))),
    Mapping("de novo", ["Findings"], mapper=one(concat("/")(select("de novo/vererbt")(dict_mapping({"de novo": "yes"}, "")(nop))))),
    Mapping("mode of inheritance", mapper=constant("")),
    Mapping("ClinVar Accession ID", mapper=constant("")),
]


def get_baserow():
    bs_api = python_baserow_simple.BaserowApi(token=settings.baserow_token)

    root_data = bs_api.get_data(settings.baserow.root_table_id)

    link_tables = {}
    for link_table_name, link_table_id in settings.baserow.link_table_mappings:
        link_table_data = bs_api.get_data(link_table_id)
        link_tables[link_table_name] = link_table_data

    # expand links in all entries
    for entry_id, entry in root_data.items():
        for link_name in link_tables:
            entry[link_name] = [
                link_tables[link_name][link_id] for link_id in entry[link_name]
            ]
        entry["Medgen ID"] = f"SV-{entry_id}"
    return root_data


def transform(entry, mappings):
    result_data = {}
    for mapping in mappings:
        result_data[mapping.destination] = mapping.map(entry)

    return result_data


def check_filter(entry, filters):
    return all(entry[f.field] in f.valid_keys for f in filters)


def main(output_file: Path):

    if settings.source.type == "baserow":
        data = get_baserow()
    else:
        raise RuntimeError("Supported data sources are currently: baserow")

    result_data = []
    for entry in data.values():
        if check_filter(entry, settings.filter):
            result_entry = transform(entry, OUTPUTS_EXOMAG)
            result_data.append(result_entry)

    table = pd.DataFrame.from_records(result_data)
    table.to_excel(output_file, index=False)


if __name__ == "__main__":
    typer.run(main)
