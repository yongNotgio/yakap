import pdfplumber
import sys

def clean_text(text):
    if text is None:
        return ""
    return text.replace("'", "''").replace("\n", " ").strip()

def main():
    pdf_path = "YAKAP.pdf"
    output_prefix = "yakap_clinics"
    table_name = "yakap_clinics"
    batch_size = 100  # Number of rows per INSERT statement
    rows_per_file = 1000 # Number of records per file
    
    # Define columns based on inspection
    columns = [
        "id",
        "facility_name", 
        "tel_no", 
        "email", 
        "street", 
        "municipality", 
        "province",  # Added province
        "expire_date", 
        "sec"
    ]
    
    HEADER_MARKER = "NAME OF HEALTH FACILITY"
    
    # Create table schema string
    create_table_stmt = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    id VARCHAR(50),
    facility_name TEXT,
    tel_no VARCHAR(100),
    email VARCHAR(255),
    street TEXT,
    municipality VARCHAR(100),
    province VARCHAR(100), -- Added province
    expire_date VARCHAR(50),
    sec VARCHAR(50)
);\n"""

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"Processing {total_pages} pages...")
            
            all_rows = []
            current_province = "Unknown"
            current_region = "Unknown"
            
            for i, page in enumerate(pdf.pages):
                # Identify red text (headers) on this page
                # Red is (1.0, 0.0, 0.0) or close to it
                red_texts = []
                # Use a small tolerance for color matching if needed
                for char in page.chars:
                    color = char.get("non_stroking_color")
                    if color == (1, 0, 0) or color == [1, 0, 0]:
                        # Tag this text as red
                        # We'll group them by line later or just check if a cell contains red characters
                        pass

                tables = page.extract_tables()
                
                # To accurately find red cells, we might need to use cells data
                # But let's try a simpler approach: check if the text in the row matches 
                # characters that are red.
                
                for table in tables:
                    for row in table:
                        cleaned_row = [clean_text(cell) for cell in row]
                        
                        if HEADER_MARKER in cleaned_row:
                            continue
                            
                        if all(c == "" for c in cleaned_row):
                            continue

                        # Check if this row is a "Red" header
                        # Instead of just checking if col 0 has text, we look for matches in the page.chars
                        cell_text = cleaned_row[0]
                        if cell_text and all(c == "" for c in cleaned_row[1:]):
                            # Verify if this text is red on this page
                            # We search for characters that match this text and check their color
                            is_red = False
                            # Simplified check: find chars on this page that match the start of cell_text
                            # and check if they are red.
                            for char in page.chars:
                                if char["text"] != " " and char["text"] in cell_text:
                                    color = char.get("non_stroking_color")
                                    if color == (1, 0, 0) or color == [1, 0, 0]:
                                        is_red = True
                                        break
                            
                            if is_red:
                                header_text = cell_text.upper()
                                if "REGION" in header_text or "ADMINISTRATIVE" in header_text:
                                    current_region = cell_text
                                else:
                                    current_province = cell_text
                                continue

                        if len(cleaned_row) < 8:
                            cleaned_row += [""] * (8 - len(cleaned_row))
                        elif len(cleaned_row) > 8:
                            cleaned_row = cleaned_row[:8]

                        # Valid rows must have an expire date and a facility name
                        if cleaned_row[6] == "" or cleaned_row[1] == "":
                            continue
                            
                        # Construct row with province
                        row_with_province = (
                            cleaned_row[:6] + 
                            [current_province] + 
                            cleaned_row[6:]
                        )
                        
                        all_rows.append(row_with_province)
                
                if (i + 1) % 10 == 0:
                    print(f"Processed {i + 1}/{total_pages} pages")

            print(f"Extraction complete. Found {len(all_rows)} records.")
            
            # Split into files
            num_files = (len(all_rows) + rows_per_file - 1) // rows_per_file
            
            for f_idx in range(num_files):
                file_start = f_idx * rows_per_file
                file_end = min((f_idx + 1) * rows_per_file, len(all_rows))
                file_rows = all_rows[file_start:file_end]
                
                output_sql = f"{output_prefix}_{f_idx + 1}.sql"
                sql_statements = []
                
                if f_idx == 0:
                    sql_statements.append(create_table_stmt)
                
                sql_statements.append("BEGIN TRANSACTION;")
                
                # Process in batches for the current file
                for b_idx in range(0, len(file_rows), batch_size):
                    batch = file_rows[b_idx : b_idx + batch_size]
                    
                    value_lists = []
                    for r in batch:
                        vals = ", ".join([f"'{v}'" for v in r])
                        value_lists.append(f"({vals})")
                    
                    values_str = ",\n".join(value_lists)
                    insert_stmt = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n{values_str};"
                    sql_statements.append(insert_stmt)
                
                sql_statements.append("COMMIT;")
                
                with open(output_sql, "w", encoding="utf-8") as f:
                    f.write("\n".join(sql_statements))
                
                print(f"Saved {len(file_rows)} records to {output_sql}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
