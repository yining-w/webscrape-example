import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
#import pdfplumber
import fitz  # PyMuPDF

# main pdf download link
path = os.getcwd()

# ----- Set up: Get a list of links ------ #
# ----- we need to do it in two iterations due to "active card" settings 
# Define the URL
url = 'https://data.ed.gov/dataset/idea-section-618-data-products-data-displays-part-b-2022/resources'

# Get the HTML content of the page
response = requests.get(url)
html_content = response.text

# Parse the HTML content
soup = BeautifulSoup(html_content, 'html.parser')

# Find all the dataset cards
dataset_cards = soup.find_all('div', class_="inner-sidebar")

# Extract all links
links_headers = []
for card in dataset_cards:
    # Find all 'a' tags within each card
    a_tags = card.find_all('a', href=True)
    for a_tag in a_tags:
        links_headers.append((a_tag['href']))

# Save the information to an Excel file
df = pd.DataFrame(links_headers, columns=['Link'])
df['Link'] = 'https://data.ed.gov' + df['Link'].astype(str)

# Remove the link with the active
df = df.drop([1])
df = df.reset_index()

# Initialize an empty dataframe for the results
results_df = pd.DataFrame(columns=['Href', 'H5'])

# Loop through each row in the dataframe
for index, row in df.iterrows():
    # Replace the 'url' with the link from the dataframe
    url = row['Link']

    # Get the HTML content of the page
    response = requests.get(url)
    html_content = response.text

    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the href within <a class="usa-button download-btn resource-type-None resource-url-analytics">
    a_tag = soup.find('a', class_="usa-button download-btn resource-type-None resource-url-analytics")
    href = a_tag['href'] if a_tag else 'No Link Found'

    # Find the <h5> element in the <div class="dataset-card active">
    dataset_card = soup.find('div', class_="dataset-card active")
    h5_tag = dataset_card.find('h5') if dataset_card else None
    h5_text = h5_tag.get_text(strip=True) if h5_tag else 'No Header Found'

    # Append the findings to the results dataframe
    results_df = results_df.append({'Href': href, 'H5': h5_text}, ignore_index=True)

# remove the unecessary parts of the string
cleaned_df = results_df
cleaned_df['H5'] = results_df['H5'].str[:-25]

# Save the excel link
df.to_excel('links_and_headers.xlsx', index=False)

# ----------- Step 2: Now, download each pdf ------------ #
for index, row in cleaned_df.iterrows():
    # Get the PDF url
    pdf_url = row['Href']

    # Make the request to get the PDF content
    response = requests.get(pdf_url)

    # Check if the request was successful
    if response.status_code == 200:
        # You can name the file using the index or another unique identifier
        # Here, I'm simply using the index to name the PDF files
        desired_name = row['H5']
        filename = desired_name + ".pdf"


        # Write the PDF content to a file in the working directory
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded and saved {filename}")
    else:
        print(f"Failed to retrieve PDF from {pdf_url}")
        

## ----------- Step 3: Read the actual table from the pdf ---------# 
## Convert to csv
files = os.listdir(path)
files = [x for x in files if '.pdf' in x]

# Empty list of dataframes to append to
dfs =[]
file = [x for x in files if '.pdf' in x]

# Use this for testing
#file = ['Wyoming.pdf']

# Function to parse the text block and extract rows as lists
def extract_rows_from_block(block):
    rows = []
    for line in block['lines']:
        # Assuming each 'line' corresponds to a row in the table
        row = [span['text'] for span in line['spans']]
        rows.append(row)
    return rows

# Loop through each file
for pdf_file in file:
    print(pdf_file)
    
    # Some pdfs are not reading the data, will need to check this
    try:
        # Open the pdf
        with fitz.open(pdf_file) as doc:
                # Access the second page of the PDF
                page = doc[1]  # Pages are zero-indexed
                # Look for the table title and extract text from there
                found_table = False
                table_blocks = []
                
                # Iterate over the text "blocks" on the page
                for block in page.get_text("dict")["blocks"]:
                    # Check if this block contains the table title
                    if any("PERCENT OF CHILDREN" in span['text'] for span in block.get("lines", [])[0].get("spans", [])):
                        found_table = True
                    if found_table and 'lines' in block:  # Assuming that the table starts after the title
                        table_blocks.append(block)
                
                # Extract rows from blocks assumed to be table data
                table_rows = []
                for block in table_blocks:
                    table_rows.extend(extract_rows_from_block(block))
                
                # The first row would typically be the headers, the rest are data
                headers = table_rows.pop(0)
                df = pd.DataFrame(table_rows, columns=headers)
                
                # Combine rows by concatenating the text values
                combine_indices = [(1, 2), (3, 4), (29, 30), (33, 34)]
                for index_pair in combine_indices:
                    # Concatenate the two rows and assign back to the first row index
                    # Assuming the first column is at index 0
                    df.iloc[index_pair[0], 0] = df.iloc[index_pair[0], 0] + " " + df.iloc[index_pair[1], 0]
                
                # Drop the second row of each pair since it's now combined with the first
                rows_to_drop = [index for pair in combine_indices for index in pair[1:]]
                df.drop(rows_to_drop, inplace=True)
                
                # Reset the index if needed
                df.reset_index(drop=True, inplace=True)
                
                # Drop the range of rows from 43 to 55 (adjusting for zero-based index)
                df.drop(df.index[39:51], inplace=True)
                
                # Assuming `df` is your DataFrame with a single column named 'Data'
                # Convert the column to a list, skipping the first three values (header row)
                data_values = df.iloc[3:, 0].tolist()
                
                # Determine how many full rows of data you have (each row has 3 values)
                num_rows = len(data_values) // 3
                
                # Reshape the data into a new DataFrame with 3 columns
                reshaped_data = [data_values[i:i + 3] for i in range(0, num_rows * 3, 3)]
                new_df = pd.DataFrame(reshaped_data, columns=df.iloc[0:3, 0].tolist())
    
                
                new_df['state'] = pdf_file
                
                dfs.append(new_df)

    # Skip the pdf if table not found
    except Exception:
        print("Skipped ",pdf_file)
        continue  # Skip to the next row
        
# Concatenate all dataframes into one
final_df = pd.concat(dfs, ignore_index=True)

# Save the cleaned thing
final_df.to_excel('final_df.xlsx', index=False)
