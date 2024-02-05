#!/usr/bin/env python3


import pandas as pd
import numpy as np
from util import (
    abbr_to_full_year,
    drop_zeroed_rows,
    fuzzy_to_name_ratio,
    get_worksheet_data,
    normalize_dates,
    numeric_to_text_month,
)


def pipeline():
    df = pd.DataFrame(get_worksheet_data())
    df = drop_zeroed_rows(df)
    df = abbr_to_full_year(df)
    df = normalize_dates(df)
    df = numeric_to_text_month(df)
    df = fuzzy_to_name_ratio(df)
    return df


def main():
    df = pipeline()
    # Quais os 5 melhores produtos em termos de faturamento (Receita) - por mês;
    df_monthly_best = (
        df.get(["Objeto", "Receita"]).groupby(by=["Objeto"])["Receita"].sum().div(12)
    )
    print(df_monthly_best.sort_values(ascending=False).head(5))

    # Quais os top 5 produtos que menos trazem cliques para o site, por mês?
    df_monthly_worst = (
        df.get(["Objeto", "Cliques"]).groupby(by=["Objeto"])["Cliques"].sum().div(12)
    )
    print(df_monthly_worst.sort_values().head(5))

    # Quais 5 produtos tem o melhor valor médio (Receita) por transação - no ano;
    df_yearly_best = (
        df.get(["Objeto", "Ticket médio"]).groupby(by=["Objeto"])["Ticket médio"].sum()
    )
    print(df_yearly_best.sort_values(ascending=False).head(5))


if __name__ == "__main__":
    main()
