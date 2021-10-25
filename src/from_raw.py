"""
Saved extraction functions for dataset
"""
from collections import OrderedDict
from functools import reduce
from typing import Iterable, List, Mapping, Set, Tuple

import numpy as np
import pandas as pd

from datatypes import ColHeader
from utils import convert2defaults, percentInequality2FloatProportion

def schools_lea_years() -> pd.DataFrame:
    columns = OrderedDict({
        0: "year",
        2: "lea_id", 
        3: "lea_name",
        4: "id",
        5: "name",
        6: "county",
    })

    df = pd.concat(tuple(
        pd.read_excel(
            f'../data/graduation/xlsx/cohort_{year}_{cohort}year.xls',
            sheet_name='School',
            names=columns.values(),
            usecols=columns.keys(),
        ) for year in range(2010, 2020) for cohort in [4, 5]
    ))

    # collect the first and last duplicate records which are preordered by year
    df_start = df.drop_duplicates(subset=['id'], keep='first')
    df_end = df.drop_duplicates(subset=['id'], keep='last')

    return (
        # merge the start and end year records
        pd.merge(
            df_start,
            df_end[['id', 'year']],
            on='id', suffixes=('_start', '_end')
        )[[
            # reorder columns
            'id', 'name', 'lea_id', 'lea_name',
            'county', 'year_start', 'year_end'
        ]]
    )

def graduation_rates() -> pd.DataFrame:
    """
    Imports data from graduation_rate/ subdirectory
    """
    columns = OrderedDict({
        0: "year",
        1: "length",
        4: "id",
        6: "county",
        7: "subgroup",
        8: "# graduated",
        9: "# in cohort",
        10: "% graduated",
    })

    converters = {
        **{ idx: convert2defaults for idx in [5, 6] },
        **{ idx: lambda val: convert2defaults(val, float) for idx in [7] },
    }

    index_columns = [0, 3, 2, 1]

    return pd.concat(tuple(
        pd.read_excel(
            f'../data/graduation/xlsx/cohort_{year}_{cohort}year.xls',
            names=columns.values(),
            usecols=columns.keys(),
            index_col=index_columns,
            converters=converters,
        ) for year in range(2010, 2018) for cohort in [4, 5]
    ))

def enrollment_dfs() -> Mapping[str, pd.DataFrame]:
    """ Get all the newest enrollment statistics """
    YEARS = filter(lambda y: y != 2018, range(2010, 2022))  # '10-'21 (NOT '18)
    GRADES = range(1, 13)
    ID_COLS = ['school_id', 'district_id']
    INFO_COLS = ['school_name', 'district_name', 'school_ctds']
    GRADE_COLS = ["PS", "KG", *GRADES]
    GEN_COLS = ["female", "male"]
    ETH_COLS = [
        "asian", "native", "black", "latino",
        "white", "islander", "multiracial"
    ]

    def append_suffix_label(
        col: str,
        label: str,
        to_cols: List[str] = ["total"]
    ) -> str:
        """ Append a suffix `label` to a column value `col` in `to_cols` """
        return col + label if col in to_cols else col

    def label_concat_cols(
        *groups: Tuple[str, List[ColHeader]]
    ) -> Set[ColHeader]:
        """ Handle suffixing of specified (ideally common) columns """
        return {
            map(lambda col: append_suffix_label(col, label), group)
            for label, group in groups
        }

    def read_excel_clean(
        year: int,
        sheet: str,
        data_columns: Iterable[str],
    ) -> pd.DataFrame:
        """ Abstracted logic of reading data with basic cleaning """
        ext = '.xls' if year in [2013, 2014] else '.xlsx'
        ctds_type = (
            {'school_ctds': np.uint64} if year in range(2012, 2017) else {}
        )
        df = pd.read_excel(
            f'../data/enrollment/xlsx/enrollment_{year}{ext}',
            sheet_name=sheet,
            converters={col: convert2defaults for col in data_columns}
        )
        df = df.dropna(subset=['school_name'])
        df = df.astype({
            **{col: np.uint16 for col in data_columns},
            **{col: np.uint32 for col in ['school_id', 'district_id']},
            **ctds_type
        })
        df['year'] = df.index.map(lambda _: year)
        return df

    # coalesce conflicting "total" columns
    geneth_cols = GEN_COLS + ETH_COLS + ['total']
    gradegen_cols = GEN_COLS + GRADE_COLS + ['total']

    # separate views
    schoolinfo = pd.DataFrame(columns=['year'] + ID_COLS + INFO_COLS)
    gradegender = pd.DataFrame(columns=['year'] + ID_COLS + gradegen_cols)
    ethsubgroups = pd.DataFrame(
        columns=['year'] + ID_COLS + ['subgroup'] + ETH_COLS + ['total']
    )
    # older years actually provide subgrouped gender alongside ethnicity
    gensubgroups = pd.DataFrame(
        columns=['year'] + ID_COLS + ['subgroup'] + geneth_cols
    )

    for year in YEARS:
        df_grade = read_excel_clean(year, 'SchoolGrade', GRADE_COLS + ['total'])

        if year <= 2012:
            df_geneth = read_excel_clean(year, 'SchoolSexEthnic', geneth_cols)
            df_gen = df_geneth[
                filter(
                    lambda col: col not in ETH_COLS,
                    df_geneth.columns.values
                )
            ]
            df_eth = df_geneth[
                filter(
                    lambda col: col not in GEN_COLS,
                    df_geneth.columns.values
                )
            ]
            gensubgroups = pd.concat((gensubgroups, df_geneth))
        else:
            df_eth = read_excel_clean(
                year, 'SchoolEthnicitySubgroup', ETH_COLS + ['total']
            )
            if year != 2013:
                df_gen = read_excel_clean(
                    year, 'SchoolGender', GEN_COLS + ['total']
                )

        schoolinfo = pd.concat((
            schoolinfo,
            df_grade[filter(
                lambda col: col in df_grade.columns.values,
                schoolinfo.columns
            )]
        ))
        ethsubgroups = pd.concat((ethsubgroups, df_eth))
        print(year)
        if year == 2013:
            gradegender = pd.concat((
                gradegender,
                df_grade[['year'] + ID_COLS + GRADE_COLS + ['total']]
            ))
        else:
            gradegender = pd.concat((
                gradegender,
                pd.merge(
                    df_grade[['year'] + ID_COLS + GRADE_COLS + ['total']],
                    df_gen[['year'] + ID_COLS + GEN_COLS + ['total']],
                    on='school_id',
                    suffixes=('_grade', '_gender')
                )
            ))

    # the "Total" column appears to not reflect the sum of the
    # counts in each grade so this "Missing" column keeps track
    # of which ones don't add up
    gradegender['missing_grade'] = (
        gradegender['total_grade'] - gradegender[GRADE_COLS[:-1]].sum(axis=1)
    )
    gradegender['missing_gen'] = (
        gradegender['total_gen'] - gradegender[GEN_COLS[:-1]].sum(axis=1)
    )
    ethsubgroups['missing_eth'] = (
        ethsubgroups['total_eth'] - ethsubgroups[ETH_COLS[:-1]].sum(axis=1)
    )
    gensubgroups['missing_eth'] = (
        gensubgroups['total_eth'] - gensubgroups[ETH_COLS[:-1]].sum(axis=1)
    )
    gensubgroups['missing_gen'] = (
        gensubgroups['total_gen'] - gensubgroups[GEN_COLS[:-1]].sum(axis=1)
    )

    return {
        "schoolinfo": schoolinfo,
        "gradegender": gradegender,
        "ethsubgroups": ethsubgroups,
        "gensubgroups": gensubgroups,
    }

def freelunch() -> pd.DataFrame:
    """
    Imports data from freelunch/ subdirectory
    """
    columns = [
        "sponsor_name",
        "sponsor_ctds",
        "site_name",
        "site_ctds",
        "sponsor_id",
        "site_id",
        "pgmpart",
        "enrollment",
        "percentage"
    ]

    converters = {
        'percentage': percentInequality2FloatProportion,
        'enrollment': convert2defaults
    }

    return pd.concat(tuple(
        pd.read_excel(
            f'../data/freelunch/xlsx/freelunch_{year}.xlsx',
            names=columns,
            converters=converters
        ) for year in range(2016, 2021)
    ))
