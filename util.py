#!/usr/bin/env python3

import re
from typing import Optional
import pandas as pd
import gspread
import datetime as dt
from thefuzz import process


def get_sheet():
    gc = gspread.service_account(filename=".gspread/service_account.json")
    return gc.open("ObjetosTeca")


def get_worksheet_data():
    worksheet = get_sheet().get_worksheet_by_id("696613527")
    return worksheet.get_all_records()


def drop_zeroed_rows(df):
    df = df.drop(
        df[
            (df.Investido == 0)
            & (df.Cliques == 0)
            & (df.Receita == 0)
            & (df.Conversões == 0)
            & (df.ROAS == 0)
            & (df["Ticket médio"] == 0)
        ].index
    )
    return df


def abbr_to_full_year(df):
    df["Ano"] = df["Ano"].mask(
        df["Ano"].astype(str).str.len() == 2, other="20" + df["Ano"].astype(str), axis=0
    )
    return df


def date_conversion(s):
    if "/" in s and int(s[:2]) <= 12:
        return pd.to_datetime(s, format="%m/%d/%Y", errors="coerce").date()
    if "/" in s:
        return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce").date()
    else:
        return pd.to_datetime(s, format="%d-%m-%Y", errors="coerce").date()


def normalize_dates(df):
    # df["Data"] = pd.to_datetime(df["Data"], errors="Coerce")
    df["Data"] = df["Data"].map(date_conversion)
    return df


def overwrite_numeric_months(df):
    num_month = (
        df[df["Mês"].apply(lambda x: not isinstance(x, str))]
        .get(["Data", "Mês"])
        .apply(
            lambda x: [x["Data"], str(x["Data"].month)],
            axis=1,
            result_type="broadcast",
        )
    )
    df.update(
        num_month,
    )
    return df


def numeric_to_text_month(df):
    df = overwrite_numeric_months(df)
    df["Mês"] = df["Mês"].map(
        lambda x: x
        if not x.isdecimal()
        else [
            "Janeiro",
            "Fevereiro",
            "Março",
            "Abril",
            "Maio",
            "Junho",
            "Julho",
            "Agosto",
            "Setembro",
            "Outubro",
            "Novembro",
            "Dezembro",
        ][int(x) - 1],
    )
    return df


def fuzzy_to_name_ratio(df):
    df["Objeto"] = df["Objeto"].str.upper()
    PRODUCTS = [
        "AGULHA",
        "APITO",
        "BICICLETA",
        "BOLSA",
        "BOTA",
        "CACHIMBO",
        "CADERNO",
        "CANETA",
        "CARRO",
        "CELULAR",
        "CLIPS",
        "COPO",
        "DADO",
        "DISCO",
        "FONE DE OUVIDO",
        "LANTERNA",
        "LIVRO",
        "MEIA",
        "MOCHILA",
        "MOEDA",
        "MOUSE",
        "ÓCULOS",
        "PIANO",
        "RÁDIO",
        "RÉGUA",
        "TECLADO",
        "TELEVISÃO",
        "TÊNIS",
        "XADREZ",
        "XÍCARA",
    ]
    df["Objeto"] = df["Objeto"].map(lambda x: process.extractOne(x, PRODUCTS)[0])
    return df
