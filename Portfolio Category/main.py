import pandas as pd

class DataLoader:
    def __init__(self, file_path, sheet_name, columns=None, skiprows=0):
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.columns = columns
        self.skiprows = skiprows

    def load_data(self):
        return pd.read_excel(self.file_path, sheet_name=self.sheet_name, usecols=self.columns, skiprows=self.skiprows)

class DataProcessor:
    def __init__(self, loandb, agunan_property_only, esinsial_1, esinsial_2, agunankreditpembiayaan):
        self.loandb = loandb
        self.agunan_property_only = agunan_property_only
        self.esinsial_1 = esinsial_1
        self.esinsial_2 = esinsial_2
        self.agunankreditpembiayaan = agunankreditpembiayaan

    def merge_data(self):
        #merged_data = pd.merge(self.loandb, self.agunan_property_only, left_on='ACCOUNT_NUMBER', right_on='NOMOR REKENING', how='left')
        #merged_data = pd.merge(merged_data, self.agunankreditpembiayaan, left_on='ACCOUNT_NUMBER', right_on='NOMOR REKENING', how='left')
        agunan_gabungan = pd.merge(
            self.agunankreditpembiayaan, 
            self.agunan_property_only, 
            on='NOMOR AGUNAN',  # Kolom untuk join
            how='inner'  # Tipe join (inner join)
        )
        
        # Daftar nilai untuk filter
        filter_jenis_agunan = [
            'AN020101', 'AN02010201', 'AN02010202', 'AN02010203', 
            'AN02010204', 'AN02010299', 'AN02010301', 'AN02010302'
        ]

        # Melakukan filter pada kolom JENIS AGUNAN dan membuat salinan DataFrame
        agunan_property_only = agunan_gabungan[agunan_gabungan['JENIS AGUNAN'].isin(filter_jenis_agunan)].copy()

        # Fungsi untuk menentukan nilai 'CEK AGUNAN'
        def cek_agunan(jenis_agunan):
            if jenis_agunan in ['AN020101', 'AN02010201', 'AN02010202', 'AN02010203', 'AN02010204', 'AN02010299']:
                return 'Beragun Properti Komersial'
            elif jenis_agunan in ['AN02010301', 'AN02010302']:
                return 'Beragun Rumah Tinggal'
            else:
                return 'Take Out'

        # Tambahkan kolom 'CEK AGUNAN' menggunakan apply
        agunan_property_only['CEK AGUNAN'] = agunan_property_only['JENIS AGUNAN'].apply(cek_agunan)
        agunan_property_only = agunan_property_only.reset_index(drop=True)

        # Agregasi nilai 'NILAI AGUNAN' berdasarkan 'NOMOR REKENING' dan 'CEK AGUNAN'
        agg = agunan_property_only.groupby(['NOMOR REKENING', 'CEK AGUNAN'])['NILAI AGUNAN'].sum().unstack(fill_value=0)

        # Gabungkan hasil agregasi ke DataFrame asli, reset index untuk menggabungkan dengan kolom NOMOR REKENING
        agunan_property_only = agunan_property_only.merge(agg[['Beragun Properti Komersial', 'Beragun Rumah Tinggal']], 
                                                          on='NOMOR REKENING', 
                                                          how='left')
        
        agunan_property_only_cleaned = agunan_property_only.drop_duplicates(subset='NOMOR REKENING', keep='first')

        # Melakukan left join pada loandb dan agunan
        data = pd.merge(
            self.loandb,
            agunan_property_only_cleaned,
            left_on='ACCOUNT_NUMBER',
            right_on='NOMOR REKENING',
            how='left'
        )

        return data

    def add_cek_agunan_new(self, data):
        data['CEK_AGUNAN_NEW'] = data['FINAL COLLATERAL'].apply(lambda x: x if pd.notna(x) else 'Non Beragun Properti')
        return data

    def add_cek_katpor_essential(self, data):
        # Pastikan tidak ada NaN sebelum konversi
        data['CIF_MASTER'] = pd.to_numeric(data['CIF_MASTER'], errors='coerce').fillna(0).astype(int)

        # Melakukan left join antara 'data' dan 'esinsial_1'
        data = pd.merge(
            data,
            self.esinsial_1[['ID_DEBITUR', 'KATPOR UPDATE']], 
            left_on='CIF_MASTER',
            right_on='ID_DEBITUR',
            how='left'
        )

        # Menambahkan kolom 'CEK_KATPOR_ESSENTIAL'
        data['CEK_KATPOR_ESSENTIAL'] = data['KATPOR UPDATE'].apply(
            lambda x: x if pd.notna(x) else 'Cek'
        )

        # Daftar kolom yang akan dihapus
        columns_to_drop = [
            'NOMOR REKENING', 'NOMOR AGUNAN', 'JENIS AGUNAN', 'NILAI AGUNAN', 'CEK AGUNAN',
            'Beragun Properti Komersial', 'Beragun Rumah Tinggal', 'FINAL COLLATERAL', 
            'NOMOR_REKENING', 'ID_DEBITUR', 'NAMA', 'KATPOR UPDATE', 'Sandi Jenis Debitur'
        ]

        # Menghapus kolom dari DataFrame
        data = data.drop(columns=columns_to_drop, errors='ignore')

        return data

    def add_cek_goldeb(self, data):
        # Langkah 1: Lakukan join sementara dengan esinsial_2
        data = pd.merge(
            data, 
            self.esinsial_2[['Sandi Referensi', 'Kode']], 
            left_on='DEBTOR_CLASSIFICATION', 
            right_on='Sandi Referensi', 
            how='left'
        )

        # Langkah 2: Tambahkan kolom 'CEK_GOLDEB' berdasarkan logika
        data['CEK_GOLDEB'] = data.apply(
            lambda row: row['CEK_KATPOR_ESSENTIAL'] 
            if row['CEK_KATPOR_ESSENTIAL'] != 'Cek' 
            else (row['Kode'] if pd.notna(row['Kode']) else 'Cek'), 
            axis=1
        )

        # Hapus kolom tambahan jika tidak dibutuhkan lagi
        data.drop(columns=['Sandi Referensi', 'Kode'], inplace=True)

        return data
    
    def handle_na_dates(self, data):
        data['START'] = pd.to_datetime(data['START'], errors='coerce')
        data['END'] = pd.to_datetime(data['END'], errors='coerce')
        
        max_start_date = data['START'].max()
        max_end_date = data['END'].max()

        data['START'] = data['START'].fillna(max_start_date)
        data['END'] = data['END'].fillna(max_end_date)

        return data

    def add_jangka_column(self, data):
        def calculate_jangka(row):
            if row['CEK_GOLDEB'] == "Cek Jangka - 17/18" and (row['END'] - row['START']).days > 90:
                return 18
            elif row['CEK_GOLDEB'] == "Cek Jangka - 17/18" and (row['END'] - row['START']).days <= 90:
                return 17
            elif row['CEK_GOLDEB'] == "Cek Jangka - 14/15" and (row['END'] - row['START']).days > 90:
                return 15
            elif row['CEK_GOLDEB'] == "Cek Jangka - 14/15" and (row['END'] - row['START']).days <= 90:
                return 14
            else:
                return row['CEK_GOLDEB']
        
        data['JANGKA'] = data.apply(calculate_jangka, axis=1)
        return data

    def add_pensiun_column(self, data):
        def calculate_pensiun(row):
            if row['JANGKA'] == "Cek" and row['PRODUCT_NEW'] == "PERSONAL":
                return 40
            else:
                return row['JANGKA']
        
        data['PENSIUN'] = data.apply(calculate_pensiun, axis=1)
        return data

    def add_cek_tanah_essential(self, data):
        def cek_tanah_essential(row):
            if row['PENSIUN'] == "Cek" and row['RESTRUCTURED_OR_NORMAL'] == 63:
                return row['RESTRUCTURED_OR_NORMAL']
            elif row['PENSIUN'] == "Cek":
                return row['KATPOR UPDATE'] if pd.notna(row['KATPOR UPDATE']) else row['PENSIUN']
            else:
                return row['PENSIUN']

        data['CEK TANAH&ESSENTIAL'] = data.apply(cek_tanah_essential, axis=1)
        return data

    def add_cek_collateral(self, data):
        def cek_collateral_logic(row):
            if row['CEK TANAH&ESSENTIAL'] == "Cek" and row['CEK_AGUNAN_NEW'] == "Beragun Rumah Tinggal":
                return 41
            elif row['CEK TANAH&ESSENTIAL'] == "Cek" and row['CEK_AGUNAN_NEW'] == "Beragun Properti Komersial":
                return 42
            else:
                return row['CEK TANAH&ESSENTIAL']

        data['CEK_COLLATERAL'] = data.apply(cek_collateral_logic, axis=1)
        return data

    def add_cek_sme(self, data):
        def cek_sme_logic(row):
            if row['CEK_COLLATERAL'] == "Cek" and (row['DEBTOR_CATEGORY'] in ["UM", "UK"] or row['DEBTOR_CLASSIFICATION'] in ["S14", "S24BL"]):
                return 36
            else:
                return row['CEK_COLLATERAL']

        data['CEK_SME'] = data.apply(cek_sme_logic, axis=1)
        return data

    def add_cek_default(self, data):
        def cek_default_logic(row):
            if (row['COLLECTIBILITY'] > 2 or row['RESTRUCTURED_OR_NORMAL'] == 1) and row['CEK_SME'] == 41 and row['TYPE_OF_LOAN'] == 3:
                return 53
            elif (row['COLLECTIBILITY'] > 2 or row['RESTRUCTURED_OR_NORMAL'] == 1) and (row['CEK_SME'] != 41 or (row['CEK_SME'] == 41 and row['TYPE_OF_LOAN'] != 3)):
                return 54
            else:
                return row['CEK_SME']

        data['CEK_DEFAULT'] = data.apply(cek_default_logic, axis=1)
        return data

    def add_final_column(self, data):
        data['FINAL'] = data['CEK_DEFAULT'].apply(lambda x: 35 if x == "Cek" else x)
        data['FINAL'] = data['FINAL'].astype('int64')
        return data

    def add_note_column(self, data):
        data['NOTE'] = data.apply(lambda row: 'True' if row['FINAL'] == row['PORTOFOLIO_CATEGORY'] else 'False', axis=1)
        return data

class ExcelWriter:
    def __init__(self, dataframe, output_file):
        self.dataframe = dataframe
        self.output_file = output_file

    def save(self):
        self.dataframe.to_excel(self.output_file, index=False)

def main():
    # Load data
    loandb_loader = DataLoader("D:/Verifikasi Kategori Portfolio Antasena Kredit/10-2024/RISK 11-2024/LOANDB OKT 2024 V1.0 (BEFORE CKPN).xlsx", "LoanDB", ["ACCOUNT_NUMBER","CIF_MASTER","NAME","DEBTOR_CATEGORY","PORTOFOLIO_CATEGORY", "RESTRUCTURED_OR_NORMAL","TYPE_OF_LOAN","START","END","COLLECTIBILITY","CURRENT_LOAN_OUTSTANDING","DEBTOR_CLASSIFICATION","PRODUCT_NEW"])
    loandb = loandb_loader.load_data()

    esinsial_1_loader = DataLoader("D:/Verifikasi Kategori Portfolio Antasena Kredit/10-2024/RISK 11-2024/Esinsial 1.xlsx", "Essential")
    esinsial_1 = esinsial_1_loader.load_data()

    esinsial_2_loader = DataLoader("D:/Verifikasi Kategori Portfolio Antasena Kredit/10-2024/RISK 11-2024/Esinsial 2.xlsx", "Essential")
    esinsial_2 = esinsial_2_loader.load_data()

    agunan_loader = DataLoader("D:/Verifikasi Kategori Portfolio Antasena Kredit/10-2024/RISK 11-2024/Report_DataPelaporan_Agunan.xlsx", "Report_DataPelaporan_Agunan", ["NOMOR AGUNAN", "JENIS AGUNAN", "NILAI AGUNAN"], skiprows=6)
    agunan_property_only = agunan_loader.load_data()

    agunankreditpembiayaan_loader = DataLoader("D:/Verifikasi Kategori Portfolio Antasena Kredit/10-2024/RISK 11-2024/Report_DataPelaporan_Agunan_KreditPembiayaan.xlsx", "Report_DataPelaporan_Agunan_Kre", ["NOMOR REKENING", "NOMOR AGUNAN"], skiprows=6)
    agunankreditpembiayaan = agunankreditpembiayaan_loader.load_data()

    # Data processing
    data_processor = DataProcessor(loandb, agunan_property_only, esinsial_1, esinsial_2, agunankreditpembiayaan)
    merged_data = data_processor.merge_data()
    merged_data = data_processor.add_cek_agunan_new(merged_data)
    merged_data = data_processor.add_cek_katpor_essential(merged_data)
    merged_data = data_processor.add_cek_goldeb(merged_data)
    merged_data = data_processor.handle_na_dates(merged_data)
    merged_data = data_processor.add_jangka_column(merged_data)
    merged_data = data_processor.add_pensiun_column(merged_data)
    merged_data = data_processor.add_cek_tanah_essential(merged_data)
    merged_data = data_processor.add_cek_collateral(merged_data)
    merged_data = data_processor.add_cek_sme(merged_data)
    merged_data = data_processor.add_cek_default(merged_data)
    merged_data = data_processor.add_final_column(merged_data)
    merged_data = data_processor.add_note_column(merged_data)

    # Save results
    excel_writer = ExcelWriter(merged_data, "D:/Verifikasi Kategori Portfolio Antasena Kredit/10-2024/RISK 11-2024/worksheet_katpor.xlsx")
    excel_writer.save()

    print("Data processing and saving complete!")

if __name__ == "__main__":
    main()
