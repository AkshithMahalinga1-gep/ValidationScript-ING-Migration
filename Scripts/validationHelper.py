# --- Fetch Control Forms ---
def validate_response_data(validation_logs, formExcel, matching_form_mongo, response_df_excel, form_responses_mongo):
    # print("Form Excel", formExcel)
    # print("Type of Form Excel", type(formExcel))
    # print("Response Excel", response_df_excel)
    # print("Type of Response Excel", type(response_df_excel))

    print("Debug: Form Excel:")
    print("Available columns in formExcel:", formExcel.columns.tolist() if hasattr(formExcel, 'columns') else formExcel.keys())

    # Filter rows from response_df_excel based on the criteria
    try:
        print("Debug: Form Excel Reference ID*:", formExcel["Form Excel Reference ID*"])
    except KeyError:
        print("Error: 'Form Excel Reference ID*' not found in formExcel")
    # print("Debug: Form Recurrence ID*:", formExcel["Form Recurrence ID*"])
    # print("Debug: Response DF Reference ID*:", response_df_excel["Reference ID*"].tolist())
    # print("Debug: Response DF Recurrence ID*:", response_df_excel["Form Recurrence ID*"].tolist())
    
    filtered_rows = response_df_excel[
        (response_df_excel["Reference ID*"] == formExcel["Reference ID*"]) &
        (response_df_excel["Form Recurrence ID*"] == formExcel["Form Recurrence ID*"])
    ]

    # Print the filtered rows
    if filtered_rows.empty:
        print("Filtered Rows: No matching rows found. Please check the filtering criteria.")
    else:
        print("Filtered Rows:")
        print(filtered_rows)
    # print("Response Mongo", form_responses)
    return ""