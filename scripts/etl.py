import requests
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, inspect, ForeignKeyConstraint, \
    Index
import logging


class CineDataEtl:

    def __init__(self):
        # Initialize paths
        self.base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.logs_path = os.path.join(self.base_dir, 'logs', 'etl_pipeline.log')
        self.env_path = os.path.join(self.base_dir, 'config', 'config.env')

        # API-related attributes
        self.base_url = 'https://api.themoviedb.org/3'
        self.api_key = None

        # Database-related attributes
        self.engine = None
        self.metadata = None

        # Configure logging and load environment variables
        self.configure_logging()
        self.load_environment()

        # Setup database
        self.setup_database()

    def configure_logging(self):
        # Create log directory if it does not exist
        log_dir = os.path.dirname(self.logs_path)
        os.makedirs(log_dir, exist_ok=True)

        # Get root logger and set the logging level to DEBUG
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # Remove existing handlers to avoid conflicts
        if logger.hasHandlers():
            logger.handlers.clear()

        # Create file handler
        file_handler = logging.FileHandler(self.logs_path)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add file handler to logger
        logger.addHandler(file_handler)

    def load_environment(self):
        # Load environment variables from config.env file
        load_dotenv(self.env_path)
        self.api_key = os.getenv('API_KEY')

    def setup_database(self):
        # Setup SQLAlchemy engine and metadata for the PostgreSQL database
        self.engine = create_engine('postgresql://postgres:postgres@localhost/cinedata')
        self.metadata = MetaData()

    @staticmethod
    def check_status_code(response):
        if response.status_code == 200:
            logging.info('Status code 200: OK')
            return True
        elif response.status_code == 404:
            logging.error('Error: Resource not found (404)')
        elif response.status_code == 500:
            logging.error('Error: Server error (500)')
        else:
            logging.error(f'Error: Received unexpected status code {response.status_code}')
        return False

    def build_url(self, endpoint, page=None):
        # Construct the URL for the API request with optional paging
        url = f'{self.base_url}{endpoint}?api_key={self.api_key}'
        if page is not None:
            url += f'&page={page}'
        return url

    def extract_data(self, endpoint, page=None):
        # Make an API request and extract data from the specified endpoint and page
        url = self.build_url(endpoint, page)
        try:
            start_time = datetime.now()
            response = requests.get(url)
            end_time = datetime.now()
            logging.debug(f'Response time for {url}: {end_time - start_time}')
            if self.check_status_code(response):
                logging.info(f'Successfully extracted data from {url}')
                return response.json()
        except requests.RequestException as e:
            logging.error(f'Network error occurred while fetching data from {url}: {e}')
        return None

    def collect_data(self, endpoint, first_page, last_page):
        # Collect data from multiple pages of an API endpoint
        data = []
        logging.info(f'Collecting data from page {first_page} to {last_page}')
        for page in range(first_page, last_page + 1):
            response = self.extract_data(endpoint, page)
            if response:
                logging.info(f'Data collected for page {page}')
                items = response.get('results', [])
                data.extend(items)
            else:
                logging.warning(f'No data collected for page {page}')
        return data

    @staticmethod
    def check_essential_fields(movie):
        # Ensure that fields exist
        essential_fields = ['title', 'release_date']
        return all(movie.get(field) for field in essential_fields)

    @staticmethod
    def check_release_date(movie):
        # Validate the format of the release date
        try:
            datetime.strptime(movie.get('release_date'), '%Y-%m-%d')
            return True
        except (TypeError, ValueError):
            return False

    @staticmethod
    def check_numeric_fields(movie):
        # Ensure that fields are numeric
        numeric_fields = ['vote_average', 'vote_count', 'popularity']
        return all(isinstance(movie.get(field), (int, float)) for field in numeric_fields)

    @staticmethod
    def clean_overview(movie):
        # Strip extra spaces from overview field
        text = movie.get('overview', '')
        return text.strip() if text else ''

    def cleanup_data(self, raw_data):
        cleaned_data = []
        seen_ids = set()
        logging.info('Starting data cleanup process.')
        for dataset in raw_data:
            try:
                if dataset['id'] in seen_ids:
                    logging.debug(f"Duplicate movie with id {dataset['id']} ignored.")
                    continue
                if not all([
                    self.check_essential_fields(dataset),
                    self.check_release_date(dataset),
                    self.check_numeric_fields(dataset)
                ]):
                    logging.warning(f"Movie with id {dataset['id']} failed validation.")
                    continue
                dataset['overview'] = self.clean_overview(dataset)
                cleaned_data.append(dataset)
                seen_ids.add(dataset['id'])
            except Exception as e:
                logging.error(f"Error during cleanup for movie ID {dataset.get('id', 'Unknown')}: {e}")
        logging.info('Data cleanup completed.')
        return cleaned_data

    @staticmethod
    def filter_data(cleaned_data, genres_list):
        # Filter and structure the data into separate DataFrames

        # Dataframe for movies table
        df_movies = pd.DataFrame(cleaned_data)
        df_movies_filtered = df_movies[
            ['id', 'title', 'overview', 'release_date', 'popularity', 'vote_average', 'vote_count']]

        # Dataframe for genres table
        df_genres = pd.DataFrame(genres_list)

        # Dataframe for movie-genres relations table
        movie_genres_list = []
        for _, row in df_movies.iterrows():
            for genre_id in row['genre_ids']:
                movie_genres_list.append({'movie_id': row['id'], 'genre_id': genre_id})
        df_movie_genres = pd.DataFrame(movie_genres_list)

        return df_movies_filtered, df_genres, df_movie_genres

    def create_tables(self):
        # Create the tables if they don't already exist

        logging.info('Creating tables in the database.')
        inspector = inspect(self.engine)

        # Create movies table
        if not inspector.has_table('movies'):
            movies_table = Table('movies', self.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('title', String),
                                 Column('overview', String),
                                 Column('release_date', String),
                                 Column('popularity', Float, index=True),
                                 Column('vote_average', Float),
                                 Column('vote_count', Integer))

        # Create genres table
        if not inspector.has_table('genres'):
            genres_table = Table('genres', self.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('name', String, index=True))

        # Create movie-genres relations table
        if not inspector.has_table('movie_genres'):
            movie_genres_table = Table('movie_genres', self.metadata,
                                       Column('movie_id', Integer),
                                       Column('genre_id', Integer),
                                       ForeignKeyConstraint(['movie_id'], ['movies.id']),
                                       ForeignKeyConstraint(['genre_id'], ['genres.id']))

            Index('Idx_movie_genre', movie_genres_table.c.movie_id, movie_genres_table.c.genre_id, unique=True)

        self.metadata.create_all(self.engine)
        logging.info('Tables created successfully.')

    def load_data_to_db(self, movies_df, genres_df, movie_genres_df):
        # Load cleaned, new data into the database

        try:
            # Add new movies
            existing_movie_ids = pd.read_sql("SELECT id FROM movies", self.engine)['id'].tolist()
            new_movies_df = movies_df[~movies_df['id'].isin(existing_movie_ids)]

            if not new_movies_df.empty:
                new_movies_df.to_sql('movies', self.engine, if_exists='append', index=False)
                logging.info(f'{len(new_movies_df)} new movies loaded into the database.')
            else:
                logging.info('No new movies to load.')

            # Add new genres
            existing_genre_ids = pd.read_sql("SELECT id FROM genres", self.engine)['id'].tolist()
            new_genres_df = genres_df[~genres_df['id'].isin(existing_genre_ids)]

            if not new_genres_df.empty:
                new_genres_df.to_sql('genres', self.engine, if_exists='append', index=False)
                logging.info(f'{len(new_genres_df)} new genres loaded into the database')
            else:
                logging.info('No new genres to load.')

            # Add new movie-genre relations
            existing_movie_genres = pd.read_sql("SELECT movie_id, genre_id FROM movie_genres", self.engine)
            new_movie_genres_df = movie_genres_df.merge(existing_movie_genres,
                                                        on=['movie_id', 'genre_id'],
                                                        how='left',
                                                        indicator=True)
            new_movie_genres_df = new_movie_genres_df[new_movie_genres_df['_merge'] == 'left_only']
            new_movie_genres_df.drop(columns=['_merge'], inplace=True)

            if not new_movie_genres_df.empty:
                new_movie_genres_df.to_sql('movie_genres', self.engine, if_exists='append', index=False)
                logging.info(f'{len(new_movie_genres_df)} new movie-genre relations loaded')
            else:
                logging.info(f'No new movie-genre relations to load.')

        except Exception as e:
            logging.error(f'Failed to load data into the database: {e}')


if __name__ == "__main__":
    logging.info('ETL process started.')
    cine_etl = CineDataEtl()

    # Extract data
    movies = cine_etl.collect_data('/movie/popular', 1, 25)
    genres = cine_etl.extract_data('/genre/movie/list')['genres']

    # Clean and filter
    cleaned_movies = cine_etl.cleanup_data(movies)
    movies_data, genres_data, movie_genres_data = cine_etl.filter_data(cleaned_movies, genres)

    # Create tables
    cine_etl.create_tables()

    # Load to database
    cine_etl.load_data_to_db(movies_data, genres_data, movie_genres_data)

    logging.info('ETL process completed successfully.')
