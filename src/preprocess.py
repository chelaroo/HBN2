# UNIX timestamp
# UID Sender (Bank|Account)
# UID Receiver (Bank|Account)
# Normalized received
# Normalized paid

import pandas as pd
from currency_converter import CurrencyConverter
import sys
import pickle

def preprocess_data(file_path, output_file, output_file_continue):
    # Read data from csv
    try:
        df = pd.read_csv(file_path, header=0)
    except FileNotFoundError:
        raise FileNotFoundError("Error: File not found.")

    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    # Add column for Unix time
    df['unix_timestamp'] = (df['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

    # Add column for day of the week (0: Monday, 1: Tuesday, ..., 6: Sunday)
    df['day_of_week'] = df['Timestamp'].dt.dayofweek

    # Extract days, hours, and minutes
    df['Day'] = df['Timestamp'].dt.day
    df['Hour'] = df['Timestamp'].dt.hour
    df['Minute'] = df['Timestamp'].dt.minute

    # Create unique identifier of Bank & Account
    df['sender_uid'] = df['From Bank'].astype(str) + df['Account'].astype(str)
    df['receiver_uid'] = df['To Bank'].astype(str) + df['Account.1'].astype(str)

    # Import the column transformer and the trained model
    with open('column_transformer.pkl', 'rb') as f:
        column_transformer = pickle.load(f)

    with open('initial_model.pkl', 'rb') as f:
        model = pickle.load(f)

    # Make a copy of the df to preprocess the data using column transformer
    # we need df for sending data to cpp pattern detect
    data = df.copy()
    data = column_transformer.transform(data)

    ### define function to deal with column tranformer prefixes
    # Remove prefixes from column transformer
    def remove_prefixes(dataframe):
        prefixes = ['onehot__', 'target__', 'remainder__']
        for prefix in prefixes:
            dataframe.columns = [col.replace(prefix, '') for col in dataframe.columns]

        return dataframe
    
    data = remove_prefixes(data)    

    y_proba = model.predict_proba(data[model.booster_.feature_name()])
    data['ypred'] = y_proba
    df['ypred'] = y_proba

    threshold = df['ypred'].quantile(0.8)
    df = df[df['ypred'] >= threshold]
    data = data[data['ypred'] >= threshold]


 
    currency_symbol_dict = {'Australian Dollar': 'AUD', 'Bitcoin': 'BTC', 'Brazil Real': 'BRL', 
                            'Canadian Dollar': 'CAD', 'Euro': 'EUR', 'Mexican Peso': 'MXN', 'Ruble': 'RUB', 
                            'Rupee': 'INR', 'Saudi Riyal': 'SAR', 'Shekel': 'ILS', 'Swiss Franc': 'CHF', 
                            'UK Pound': 'GBP', 'US Dollar': 'USD', 'Yen': 'JPY', 'Yuan': 'CNY'}

    # Transform the currency in currency symbol for normalized conversion
    df['receiving_currency_symbol'] = df['Receiving Currency'].map(currency_symbol_dict)
    df['paid_currency_symbol'] = df['Payment Currency'].map(currency_symbol_dict)

    c = CurrencyConverter()

    btc_to_usd_rate = 19804.78  # 1 BTC = $19,804.78
    sar_to_usd_rate = 0.2653 # 1 SAR = $0.2653

    # Function to convert amount to USD
    def convert_to_usd(amount, currency):
        if currency == 'BTC':
            return amount * btc_to_usd_rate
        elif currency == 'SAR':
            return amount * sar_to_usd_rate
        else:
            return c.convert(amount, currency, 'USD')

    # Add columns for normalized received and paid amounts in USD
    df['normalized_received_amount'] = df.apply(lambda row: convert_to_usd(row['Amount Received'], row['receiving_currency_symbol']), axis=1)
    df['normalized_paid_amount'] = df.apply(lambda row: convert_to_usd(row['Amount Paid'], row['paid_currency_symbol']), axis=1)
    df.sort_values(by='Timestamp', inplace=True)

    df = df.drop(columns=['Timestamp', 'Account', 'Account.1'])

    # Extract columns of interest
    selected_columns = ['unix_timestamp', 'sender_uid', 'receiver_uid', 'normalized_received_amount', 'normalized_paid_amount']

    # Save selected data to CSV
    df[selected_columns].to_csv(output_file, index=False, header=False, sep=" ")
    data.to_csv(output_file_continue, index=False, header=False)

    # return data

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python preprocess_data.py <input_csv_path> <output_csv_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    try:
        preprocessed_df = preprocess_data(input_path, output_path)
        print(preprocessed_df)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)