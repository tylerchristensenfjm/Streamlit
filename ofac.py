import requests 
import xml.etree.ElementTree as ET
import pandas as pd
from rapidfuzz import fuzz
import streamlit as st
import os
# Function to download and flatten SDN XML
@st.cache_data(show_spinner=False)
def download_and_flatten_sdn():
    url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Download failed")
    root = ET.fromstring(response.content)
    ns = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}
    records = []

    for entry in root.findall('ns:sdnEntry', ns):
        first_name = entry.findtext('ns:firstName', default='', namespaces=ns)
        last_name = entry.findtext('ns:lastName', default='', namespaces=ns)
        full_name = f"{first_name} {last_name}".strip()

        akas = [
            f"{aka.findtext('ns:firstName', default='', namespaces=ns)} {aka.findtext('ns:lastName', default='', namespaces=ns)}".strip()
            for aka in entry.findall('ns:akaList/ns:aka', ns)
        ]
        akas = akas if akas else []
        all_names = [full_name] + akas
        searchable_blob = " ".join(all_names).lower()
        records.append({
            'uid': entry.findtext('ns:uid', default='', namespaces=ns),
            'firstName': first_name,
            'lastName': last_name,
            'name': full_name,
            'akas': akas,
            'all_names': all_names,
            'searchable_blob': searchable_blob,
            'sdnType': entry.findtext('ns:sdnType', default='', namespaces=ns),
            'remarks': entry.findtext('ns:remarks', default='', namespaces=ns)
        })
    return pd.DataFrame(records)

from rapidfuzz import process

def search_name(df, query, threshold=80, limit=10):
    matches = process.extract(
        query,
        df['searchable_blob'],
        scorer=fuzz.partial_ratio,
        score_cutoff=threshold,
        limit=limit
    )

    results = []
    for match_text, score, idx in matches:
        row = df.iloc[idx]
        results.append({
            'Input Name': query,
            'Matched Text': match_text,
            'Score': score,
            'Entity Name': row['name'],
            'AKAs': '; '.join(row['akas']) if isinstance(row['akas'], list) else '',
            'Type': row['sdnType'],
            'Remarks': row['remarks']
        })

    return pd.DataFrame(results).sort_values(by='Score', ascending=False)
    return df_results

# Streamlit App UI
st.set_page_config(page_title="SDN Fuzzy Search", layout="wide")
st.title("🕵️‍♂️ OFAC SDN Fuzzy Match Search")

uploaded_file = st.file_uploader("Upload a CSV file with a 'Name' column", type="csv")
manual_name = st.text_input("Or enter a single name to search (optional):", value="")
threshold = st.slider("Name Match Accuracy:", 60, 100, 85)

run_search = st.button("Start Search")

if run_search and (uploaded_file or manual_name):
        try:
            df_sdn = download_and_flatten_sdn()
            all_matches = []

            if uploaded_file:
                df_input = pd.read_csv(uploaded_file)
                if 'Name' not in df_input.columns:
                    st.error("CSV must contain a 'Name' column.")
                else:
                    for name in df_input['Name'].dropna():
                        matches = search_name(df_sdn, name, threshold)
                        if matches.empty:
                            matches = pd.DataFrame([{
                                'Input Name': name,
                                'Matched Text': '',
                                'Score': 0,
                                'Entity Name': '',
                                'AKAs': '',
                                'Type': '',
                                'Remarks': ''
                            }])
                        else:
                            matches['Input Name'] = name
                        all_matches.append(matches)

            if manual_name:
                matches = search_name(df_sdn, manual_name, threshold)
                if matches.empty:
                    matches = pd.DataFrame([{
                        'Input Name': manual_name,
                        'Matched Text': '',
                        'Score': 0,
                        'Entity Name': '',
                        'AKAs': '',
                        'Type': '',
                        'Remarks': ''
                    }])
                else:
                    matches['Input Name'] = manual_name
                all_matches.append(matches)

            if all_matches:
                result_df = pd.concat(all_matches, ignore_index=True)
                result_df['Score'] = pd.to_numeric(result_df['Score'], errors='coerce').fillna(0).astype(int)
                st.success(f"✅ Found {len(result_df[result_df['Score'] > 0])} potential match(es).")
                st.dataframe(result_df)

                csv = result_df.to_csv(index=False).encode('utf-8')
                st.download_button("Download Matches as CSV", data=csv, file_name="sdn_matches.csv", mime="text/csv")
        except Exception as e:
            st.error(f"❌ Failed to process input: {e}")
