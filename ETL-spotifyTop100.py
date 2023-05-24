import pandas as pd

data = pd.read_csv('Top 100 most Streamed - Sheet1.csv')
data = data.rename(columns={
    'beats.per.minute': 'bpm',
    'loudness.dB': 'loudness'
})
data = data.drop(['title', 'year',	'bpm', 'energy', 'danceability', 'loudness', 'liveness', 'valance', 'length', 'acousticness','speechiness', 'popularity'], axis=1, inplace=True)

data.to_csv('Artists on top 100 songs.csv', index=False)





class SpotifyTop100ETL:
    def __init__(self):
        self.Songs_data = None
        self.Artists_data = None

    def extract_Songs_data(self):
        self.Songs_data = pd.read_csv('Top 100 most Streamed - Sheet1.csv', encoding='latin-1')
        self.Songs_data.dropna(inplace=True)
        print("Songs data:")
        print(self.Songs_data.columns)

    def extract_Artists_data(self):
        self.Artists_data = pd.read_csv('Artists on top 100 songs.csv', encoding='latin-1')
        self.Artists_data.dropna(inplace=True)
        print("Artist data:")
        print(self.Artists_data.columns)

    def transform_data(self):
        merged_data = pd.merge(self.Songs_data, self.Artists_data, on='artist')
        profitable_program = merged_data.groupby('artist')['Total Songs by Artist'].idxmax()
        transformed_data = merged_data.loc[profitable_program, ['Artist', 'Total Songs by Artist']]
        return transformed_data

    def load_data(self):
        transformed_data = self.transform_data()
        print("data successfully loaded to the database.")
        print(transformed_data)

    def run_etl(self):
        self.extract_Songs_data()
        self.extract_Artists_data()
        self.load_data()


SpotifyTop100ETL.run_etl()
