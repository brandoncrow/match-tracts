import os
import pyodbc
import pandas as pd
import recordlinkage
import unicodedata
import re
from dotenv import load_dotenv

# load environmental variables
load_dotenv()
SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USERNAME = os.getenv("SQL_USERNAME")
DRIVER = os.getenv("SQL_DRIVER")

# connection string using Active Directory Authentication
CONN_STR = (
    f"DRIVER={DRIVER};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"Authentication=ActiveDirectoryInteractive;"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
)

# SQL queries
QUERY_CLIENT = """
SELECT DISTINCT 
    a.AgreementID, AgreementTractID, AgreementNumber, AgreementTractNumber, FirstParty,
    StateAbbr, County, SEC, TWP, TWPDIR, RNG, RNGDIR, Quartering, Lot, Book, [Page], [Entry], RecordingDate
FROM tmp.client_list t 
JOIN client..Agreement a ON a.AgreementNumber = t.[Agreement Number] 
JOIN client..AgreementTract tt ON a.AgreementID = tt.AgreementID;
"""

QUERY_PRODUCTION = """
SELECT DISTINCT 
    a.AgreementID, AgreementTractID, AgreementNumber, AgreementTractNumber, FirstParty,
    StateAbbr, County, SEC, TWP, TWPDIR, RNG, RNGDIR, Quartering, Lot, Book, [Page], [Entry], RecordingDate
FROM Agreement a
JOIN AgreementTract t ON a.AgreementID = t.AgreementID
WHERE StateAbbr = 'UT' AND County = 'DUCHESNE';
"""

# text cleaning
def clean_text(text):
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    text = text.replace('&', 'and').replace(',', ' and')
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()

# matching
def run_fuzzy_match():
    with pyodbc.connect(CONN_STR) as conn:
        df1 = pd.read_sql(QUERY_CLIENT, conn).reset_index(drop=True)
        df2 = pd.read_sql(QUERY_PRODUCTION, conn).reset_index(drop=True)

    # clean specified fields
    fields_to_clean = [("FirstParty", "FirstParty_clean"), ("Book", "Book_clean"),
                       ("Page", "Page_clean"), ("Entry", "Entry_clean")]

    for original, new in fields_to_clean:
        df1[new] = df1[original].astype(str).apply(clean_text)
        df2[new] = df2[original].astype(str).apply(clean_text)

    # blocking on legal land descriptions
    indexer = recordlinkage.Index()
    indexer.block(['SEC', 'TWP', 'TWPDIR', 'RNG', 'RNGDIR'])
    candidate_links = indexer.index(df1, df2)

    # compare using Jaro-Winkler similarity
    compare = recordlinkage.Compare()
    for field in [f[1] for f in fields_to_clean]:
        compare.string(field, field, method='jarowinkler', label=field)
    
    features = compare.compute(candidate_links, df1, df2)
    features['overall_similarity'] = features.mean(axis=1)

    # filter and select top matches
    if features.empty:
        print("No matches found.")
        return

    selected = features[features['overall_similarity'] > 0].reset_index()
    top_matches = selected.loc[selected.groupby("level_0")["overall_similarity"].idxmax()]

    # Merge results
    merged = df1.reset_index(drop=True).merge(top_matches, left_index=True, right_on="level_0", how="left")
    merged = merged.merge(df2.reset_index(drop=True), left_on="level_1", right_index=True,
                          suffixes=("_df1", "_df2"), how="left")

    # Select final fields for output
    output_columns = ['overall_similarity', 'AgreementID_df1', 'AgreementTractID_df1',
                      'AgreementNumber_df1', 'AgreementTractNumber_df1', 'AgreementID_df2',
                      'AgreementTractID_df2', 'AgreementNumber_df2', 'AgreementTractNumber_df2',
                      'FirstParty_df1', 'FirstParty_df2', 'FirstParty_clean_df1', 'FirstParty_clean_df2',
                      'FirstParty_clean_sim', 'Book_clean_sim', 'Page_clean_sim', 'Entry_clean_sim']

    output_columns += [col for col in merged.columns if col not in output_columns and ('_df1' in col or '_df2' in col)]

    final_df = merged[output_columns].sort_values('overall_similarity', ascending=False)
    final_df.to_csv("final_matched_records.csv", index=False)

    print("Matches saved to final_matched_records.csv")
    print(final_df.head())

if __name__ == "__main__":
    run_fuzzy_match()