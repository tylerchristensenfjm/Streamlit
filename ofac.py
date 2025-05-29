import requests 
import xml.etree.ElementTree as ET
import pandas as pd
from rapidfuzz import fuzz
import streamlit as st
import os



# ===================== DOWNLOAD THE FILE ===========================

# url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"


# # Step 1: Download the SDN.XML file
# url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
# response = requests.get(url)

# if response.status_code == 200:
#     print('Pulling data')
# else:
#     raise Exception(f"Failed to download file: Status code {response.status_code}")

# # Step 2: Parse the XML from the response
# root = ET.fromstring(response.content)

# # Step 3: Define the XML namespace
# ns = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}

# # Step 4: Extract and flatten entries
# records = []

# for entry in root.findall('ns:sdnEntry', ns):
#     base = {
#         'uid': entry.findtext('ns:uid', default='', namespaces=ns),
#         'firstName': entry.findtext('ns:firstName', default='', namespaces=ns),
#         'lastName': entry.findtext('ns:lastName', default='', namespaces=ns),
#         'sdnType': entry.findtext('ns:sdnType', default='', namespaces=ns),
#         'remarks': entry.findtext('ns:remarks', default='', namespaces=ns)
#     }

#     # Program list
#     programs = [p.text for p in entry.findall('ns:programList/ns:program', ns)]
#     base['programs'] = '; '.join(programs)

#     # Aliases
#     aka_list = entry.findall('ns:akaList/ns:aka', ns)
#     base['akas'] = '; '.join([aka.findtext('ns:lastName', default='', namespaces=ns) for aka in aka_list])

#     # ID Info
#     id_list = entry.findall('ns:idList/ns:id', ns)
#     base['id_info'] = '; '.join([
#         f"{id.findtext('ns:idType', '', namespaces=ns)}: {id.findtext('ns:idNumber', '', namespaces=ns)}"
#         for id in id_list
#     ])

#     # Vessel Info
#     vessel = entry.find('ns:vesselInfo', ns)
#     if vessel is not None:
#         base['vessel_callSign'] = vessel.findtext('ns:callSign', '', namespaces=ns)
#         base['vessel_type'] = vessel.findtext('ns:vesselType', '', namespaces=ns)
#         base['vessel_flag'] = vessel.findtext('ns:vesselFlag', '', namespaces=ns)

#     # Birth & Nationality
#     dob = entry.find('ns:dateOfBirthList/ns:dateOfBirthItem/ns:dateOfBirth', ns)
#     base['dateOfBirth'] = dob.text if dob is not None else ''

#     nationality = entry.find('ns:nationalityList/ns:nationality/ns:country', ns)
#     base['nationality'] = nationality.text if nationality is not None else ''

#     # Address List: create one row per address
#     address_list = entry.findall('ns:addressList/ns:address', ns)
#     if address_list:
#         for addr in address_list:
#             record = base.copy()
#             record['address1'] = addr.findtext('ns:address1', '', namespaces=ns)
#             record['address2'] = addr.findtext('ns:address2', '', namespaces=ns)
#             record['address3'] = addr.findtext('ns:address3', '', namespaces=ns)
#             record['city'] = addr.findtext('ns:city', '', namespaces=ns)
#             record['postalCode'] = addr.findtext('ns:postalCode', '', namespaces=ns)
#             record['country'] = addr.findtext('ns:country', '', namespaces=ns)
#             records.append(record)
#     else:
#         # No address, add a single row with missing address fields
#         base['address1'] = base['address2'] = base['address3'] = ''
#         base['city'] = base['postalCode'] = base['country'] = ''
#         records.append(base)

# # Step 5: Convert to DataFrame and export
# df = pd.DataFrame(records)
# # df.to_csv("sdn_flattened.csv", index=False)
# print(df)

# print("SDN XML flattened and saved as 'sdn_flattened.csv'")


# ================ RUN THE PROGRAM LOCALLY ==============================

# def download_and_flatten_sdn():
#     url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise Exception("Download failed")
#     root = ET.fromstring(response.content)
#     ns = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}
#     records = []

#     for entry in root.findall('ns:sdnEntry', ns):
#         first_name = entry.findtext('ns:firstName', default='', namespaces=ns)
#         last_name = entry.findtext('ns:lastName', default='', namespaces=ns)
#         full_name = f"{first_name} {last_name}".strip()

#         akas = [
#             f"{aka.findtext('ns:firstName', default='', namespaces=ns)} {aka.findtext('ns:lastName', default='', namespaces=ns)}".strip()
#             for aka in entry.findall('ns:akaList/ns:aka', ns)
# ]
#         akas = akas if akas else []
#         all_names = [full_name] + akas
#         searchable_blob = " ".join(all_names).lower()
#         records.append({
#             'uid': entry.findtext('ns:uid', default='', namespaces=ns),
#             'firstName': first_name,
#             'lastName': last_name,
#             'name': full_name,
#             'akas': akas,
#             'all_names': all_names,
#             'searchable_blob': searchable_blob,
#             'sdnType': entry.findtext('ns:sdnType', default='', namespaces=ns),
#             'remarks': entry.findtext('ns:remarks', default='', namespaces=ns)
#         })
#     return pd.DataFrame(records)

# def search_name(df, query, threshold=80):
#     results = []
#     query_lower = query.lower()
#     for _, row in df.iterrows():
#         score = fuzz.partial_ratio(query_lower, row['searchable_blob'])
#         if score >= threshold:
#             results.append({
#                 'Match': query,
#                 'Score': score,
#                 'Entity Name': row['name'],
#                 'AKAs': '; '.join(row['akas']) if isinstance(row['akas'], list) else '',
#                 'Type': row['sdnType'],
#                 'Remarks': row['remarks']
#             })
#     return pd.DataFrame(results).sort_values(by='Score', ascending=False)

# def main():
#     print("\U0001F4E5 Downloading and processing OFAC SDN List...")
#     df = download_and_flatten_sdn()
#     print("\u2705 Data ready for search.")

#     while True:
#         query = input("\n\U0001F50D Enter name to search (or type 'exit'): ").strip()
#         if query.lower() == 'exit':
#             print("\U0001F44B Exiting search.")
#             break
#         threshold_input = input("\U0001F3AF Enter match threshold (60–100, default 85): ").strip()
#         try:
#             threshold = int(threshold_input)
#             if not 60 <= threshold <= 100:
#                 threshold = 85
#         except:
#             threshold = 85

#         try:
#             results = search_name(df, query, threshold)
#         except Exception as e:
#             print(f"❌ Error during search: {e}")
#             continue

#         if results.empty:
#             print("❌ No matches found.")
#         else:
#             print(f"\n\U0001F3AF Found {len(results)} match(es):\n")
#             print(results[['Match', 'Score', 'Entity Name', 'AKAs', 'Type', 'Remarks']].to_string(index=False))

# if __name__ == "__main__":
#     main()


# ================ USE STREAMLIT LOCALLY ==============================

# @st.cache_data(show_spinner=False)
# def download_and_flatten_sdn():
#     url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise Exception("Download failed")
#     root = ET.fromstring(response.content)
#     ns = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}
#     records = []

#     for entry in root.findall('ns:sdnEntry', ns):
#         first_name = entry.findtext('ns:firstName', default='', namespaces=ns)
#         last_name = entry.findtext('ns:lastName', default='', namespaces=ns)
#         full_name = f"{first_name} {last_name}".strip()

#         akas = [
#     f"{aka.findtext('ns:firstName', default='', namespaces=ns)} {aka.findtext('ns:lastName', default='', namespaces=ns)}".strip()
#     for aka in entry.findall('ns:akaList/ns:aka', ns)
# ]
#         akas = akas if akas else []
#         all_names = [full_name] + akas
#         searchable_blob = " ".join(all_names).lower()
#         records.append({
#             'uid': entry.findtext('ns:uid', default='', namespaces=ns),
#             'firstName': first_name,
#             'lastName': last_name,
#             'name': full_name,
#             'akas': akas,
#             'all_names': all_names,
#             'searchable_blob': searchable_blob,
#             'sdnType': entry.findtext('ns:sdnType', default='', namespaces=ns),
#             'remarks': entry.findtext('ns:remarks', default='', namespaces=ns)
#         })
#     return pd.DataFrame(records)

# def search_name(df, query, threshold=80):
#     results = []
#     query_lower = query.lower()
#     for _, row in df.iterrows():
#         score = fuzz.partial_ratio(query_lower, row['searchable_blob'])
#         if score >= threshold:
#             results.append({
#                 'Matched Text': query,
#                 'Score': score,
#                 'Entity Name': row['name'],
#                 'AKAs': '; '.join(row['akas']) if isinstance(row['akas'], list) else '',
#                 'Type': row['sdnType'],
#                 'Remarks': row['remarks']
#             })
#     df_results = pd.DataFrame(results)
#     if not df_results.empty and 'Score' in df_results.columns:
#         df_results['Score'] = pd.to_numeric(df_results['Score'], errors='coerce').fillna(0).astype(int)
#         return df_results.sort_values(by='Score', ascending=False)
#     return df_results

# # Streamlit UI
# st.set_page_config(page_title="SDN Fuzzy Search", layout="wide")
# st.title("OFAC SDN Fuzzy Name Search")

# with st.spinner("Downloading and parsing SDN list..."):
#     df_sdn = download_and_flatten_sdn()

# query = st.text_input("Enter name or keyword to search:")
# threshold = st.slider("Match threshold (60–100):", min_value=60, max_value=100, value=85)

# if query:
#     results_df = search_name(df_sdn, query, threshold)
#     if results_df.empty:
#         st.warning("No matches found.")
#     else:
#         st.success(f"Found {len(results_df)} match(es)")
#         st.dataframe(results_df, use_container_width=True)


# ================== RUN AGAINST A CSV FILE OF NAMES =====================


# def download_and_flatten_sdn():
#     url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise Exception("Download failed")
#     root = ET.fromstring(response.content)
#     ns = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}
#     records = []

#     for entry in root.findall('ns:sdnEntry', ns):
#         first_name = entry.findtext('ns:firstName', default='', namespaces=ns)
#         last_name = entry.findtext('ns:lastName', default='', namespaces=ns)
#         full_name = f"{first_name} {last_name}".strip()

#         akas = [
#             f"{aka.findtext('ns:firstName', default='', namespaces=ns)} {aka.findtext('ns:lastName', default='', namespaces=ns)}".strip()
#             for aka in entry.findall('ns:akaList/ns:aka', ns)
#         ]
#         akas = akas if akas else []
#         all_names = [full_name] + akas
#         searchable_blob = " ".join(all_names).lower()
#         records.append({
#             'uid': entry.findtext('ns:uid', default='', namespaces=ns),
#             'firstName': first_name,
#             'lastName': last_name,
#             'name': full_name,
#             'akas': akas,
#             'all_names': all_names,
#             'searchable_blob': searchable_blob,
#             'sdnType': entry.findtext('ns:sdnType', default='', namespaces=ns),
#             'remarks': entry.findtext('ns:remarks', default='', namespaces=ns)
#         })
#     return pd.DataFrame(records)

# def search_name(df, query, threshold=80):
#     results = []
#     query_lower = query.lower()
#     for _, row in df.iterrows():
#         score = fuzz.partial_ratio(query_lower, row['searchable_blob'])
#         if score >= threshold:
#             results.append({
#                 'Input Name': query,
#                 'Matched Text': query,
#                 'Score': score,
#                 'Entity Name': row['name'],
#                 'AKAs': '; '.join(row['akas']) if isinstance(row['akas'], list) else '',
#                 'Type': row['sdnType'],
#                 'Remarks': row['remarks']
#             })
#     df_results = pd.DataFrame(results)
#     if not df_results.empty and 'Score' in df_results.columns:
#         df_results['Score'] = pd.to_numeric(df_results['Score'], errors='coerce').fillna(0).astype(int)
#         return df_results.sort_values(by='Score', ascending=False)
#     return df_results

# def batch_search_from_csv(input_csv, output_csv="sdn_matches.csv", threshold=85):
#     print("Reading input names from:", input_csv)
#     try:
#         df_input = pd.read_csv(input_csv)
#     except Exception as e:
#         print(f"Error reading CSV: {e}")
#         return

#     if 'Name' not in df_input.columns:
#         print("CSV must contain a 'Name' column.")
#         return

#     df_sdn = download_and_flatten_sdn()
#     all_matches = []

#     for name in df_input['Name'].dropna():
#         print(name)
#         matches = search_name(df_sdn, name, threshold)
#         if matches.empty:
#             matches = pd.DataFrame([{
#                 'Input Name': name,
#                 'Matched Text': '',
#                 'Score': 0,
#                 'Entity Name': '',
#                 'AKAs': '',
#                 'Type': '',
#                 'Remarks': ''
#             }])
#         else:
#             matches['Input Name'] = name
#         print(matches)
#         all_matches.append(matches)

#     result_df = pd.concat(all_matches, ignore_index=True)
#     result_df['Score'] = pd.to_numeric(result_df['Score'], errors='coerce').fillna(0).astype(int)
#     result_df.to_csv(output_csv, index=False)
#     print(f"✅ Matches written to: {output_csv}")

# # Run as batch process
# if __name__ == "__main__":
#     input_file = "testsdnlistinput.csv"  # Replace with your actual CSV file path
#     threshold = 85  # Set your desired threshold here
#     batch_search_from_csv(input_csv=input_file, threshold=threshold)# Run as batch process



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

def search_name(df, query, threshold=80):
    results = []
    query_lower = query.lower()
    for _, row in df.iterrows():
        score = fuzz.partial_ratio(query_lower, row['searchable_blob'])
        if score >= threshold:
            results.append({
                'Input Name': query,
                'Matched Text': query,
                'Score': score,
                'Entity Name': row['name'],
                'AKAs': '; '.join(row['akas']) if isinstance(row['akas'], list) else '',
                'Type': row['sdnType'],
                'Remarks': row['remarks']
            })
    df_results = pd.DataFrame(results)
    if not df_results.empty and 'Score' in df_results.columns:
        df_results['Score'] = pd.to_numeric(df_results['Score'], errors='coerce').fillna(0).astype(int)
        return df_results.sort_values(by='Score', ascending=False)
    return df_results

# Streamlit App UI
st.set_page_config(page_title="SDN Fuzzy Search", layout="wide")
st.title("OFAC SDN Fuzzy Match Search")

uploaded_file = st.file_uploader("Upload a CSV file with a 'Name' column", type="csv")
manual_name = st.text_input("Or enter a single name to search (optional):", value="")
threshold = st.slider("Match Threshold:", 60, 100, 85)

if uploaded_file or manual_name:
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
