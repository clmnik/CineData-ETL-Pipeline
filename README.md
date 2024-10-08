# CineData ETL Pipeline

## Project Description

This project implements an ETL (Extract, Transform, Load) pipeline to collect and process movie data using the [The Movie Database (TMDb) API](https://www.themoviedb.org/documentation/api). The pipeline extracts data on popular movies and their associated genres, cleans and transforms the data, and stores it in a PostgreSQL database. The database is designed following the principles of the Third Normal Form (3NF), ensuring an efficient and relational structure.

## Technologies Used

- **Python**: Main programming language for implementing the ETL pipeline.
- **SQLAlchemy**: Used to manage database connections, metadata, and schema creation in a PostgreSQL database.
- **PostgreSQL**: The relational database used to store movie, genre, and relationship data.
- **Pandas**: Data manipulation library used to structure and clean the data.
- **Requests**: Library to handle HTTP requests to the TMDb API.
- **Dotenv**: Used for loading environment variables such as API keys.
- **Logging**: For capturing the flow of the ETL process, logging any errors, and monitoring performance.
- **Environment Variables**: Configurations (e.g., API keys) are managed securely using environment variables.
- **3NF Database Design**: Ensures that the data stored in the database follows proper normalization rules to reduce redundancy and ensure data integrity.


### ETL Process Overview

1. **Extract**: The pipeline fetches data from the TMDb API, including popular movies and their genres.
2. **Transform**: 
    - Data is validated for essential fields (e.g., title, release date).
    - Movie data is cleaned by removing duplicates and ensuring proper formats.
    - The data is split into three logical datasets: movies, genres, and movie-genre relationships.
3. **Load**: 
    - The data is loaded into a PostgreSQL database with three tables: `movies`, `genres`, and `movie_genres`.
    - The tables are created if they do not already exist, and only new records are inserted to prevent duplication.

### Database Tables

![er_diagram](https://github.com/clmnik/CineData-ETL-Pipeline/blob/master/docs/er_diagram.png)

- **Movies Table**: Stores details about movies such as their title, release date, popularity, and votes.
- **Genres Table**: Contains a list of unique movie genres.
- **Movie-Genres Table**: Stores the relationship between movies and their genres using foreign keys to the movies and genres tables.

## How to Run the Project

### Prerequisites

- Python 3.x
- PostgreSQL installed and configured
- A TMDb API key (stored in a `.env` file)
- Required Python packages (listed in `requirements.txt`)

### Setup

1. Clone the repository to your local machine:

   ```bash
   git clone https://github.com/clmnik/CineData-ETL-Pipeline.git
   ```

2. Create a virtual environment and install dependencies:

   ```bash
   cd cinedata-etl-pipeline
   python -m venv venv
   source venv/Scripts/activate  
   pip install -r requirements.txt
   ```

3. Open the `.env` file located in the `config/` directory and replace the placeholder with your actual TMDb API key:

   ```
   API_KEY=your_tmdb_api_key
   ```

4. Set up your PostgreSQL database and ensure that the connection string in the `etl.py` file is correctly configured:

   ```python
   self.engine = create_engine('postgresql://username:password@localhost/cinedata')
   ```

5. Run the ETL pipeline:

   ```bash
   scripts/python etl.py
   ```

### Logging

The pipeline logs key activities and errors to a log file located in the `logs/` directory (`etl_pipeline.log`). This helps in monitoring the ETL process and troubleshooting any issues that arise during data extraction, transformation, or loading.

## Future Improvements

- Automate the ETL process with Apache Airflow for better scheduling and error-handling. 
- Containerize the pipeline using Docker to ensure consistent environments and easy deployment. 
- Add data visualizations with matplotlib to analyze trends and insights from the movie data.
  
