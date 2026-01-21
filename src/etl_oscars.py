import pandas as pd
import requests
import time
import os

#Setup
API_KEY = 'API KEY' 

# File Paths
INPUT_FILE = '../data/raw/the_oscar_award.csv'      # Path to Kaggle CSV
OUTPUT_MOVIES = '../data/processed/dim_movies_enriched.csv'
OUTPUT_AWARDS = '../data/processed/fact_awards.csv'

# Functions

def get_tmdb_data(title, year):

  # Fetches all the data for a movie and their respective poster
    search_url = "https://api.themoviedb.org/3/search/movie"
    details_url = "https://api.themoviedb.org/3/movie/{}"
    
    # Search parameters
    params = {'api_key': API_KEY, 'query': title, 'year': year}
    
    try:
        #Search for the Movie ID
        response = requests.get(search_url, params=params)
        results = response.json().get('results')
        
        # Fallback: If not found with exact year, try without year
        # (Ceremony year vs Release year logic can sometimes vary by >1 year)
        if not results:
            del params['year']
            response = requests.get(search_url, params=params)
            results = response.json().get('results')
            
        if results:
            movie_id = results[0]['id']
            
            # 2. Get Full Details (Budget, Revenue, Poster) using the ID
            details = requests.get(details_url.format(movie_id), params={'api_key': API_KEY}).json()
            
            # Construct full image URL
            poster_path = details.get('poster_path')
            full_poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
            
            return {
                'tmdb_id': movie_id,
                'budget': details.get('budget', 0),
                'revenue': details.get('revenue', 0),
                'runtime': details.get('runtime', 0),
                'genres': ", ".join([g['name'] for g in details.get('genres', [])]),
                'release_date': details.get('release_date'),
                'poster_url': full_poster_url, 
                'status': 'Found'
            }
    except Exception as e:
        print(f"Error searching for {title}: {e}")
        
    # If failed, return default empty structure
    return {'status': 'Not Found', 'budget': 0, 'revenue': 0, 'runtime': 0, 'poster_url': None}



if __name__ == "__main__":
    print("Starting Process...")

    # 1. Load Original CSV
    print("1. Loading raw dataset...")
    try:
        df_original = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"ERROR: Could not find {INPUT_FILE}. Check your file structure.")
        exit()

    # Basic Cleaning
    df_original['film'] = df_original['film'].str.strip()
    df_original['year_ceremony'] = df_original['year_ceremony'].fillna(0).astype(int)


    # It focus on 'Best Picture' winners to optimize API calls and demonstrate specific insights.
    print("2. Filtering for 'Best Picture' winners...")
    target_categories = ['BEST PICTURE', 'OUTSTANDING PICTURE', 'BEST MOTION PICTURE']
    
    # Filter: Category matches target AND is a winner
    df_filtered = df_original[df_original['category'].str.upper().isin(target_categories) & (df_original['winner'] == True)].copy()

    # Create unique movies list
    unique_movies = df_filtered[['film', 'year_ceremony']].drop_duplicates()
    print(f"   -> Found {len(unique_movies)} unique movies to enrich.")

    enriched_data = []

    # 3. EXTRACTION LOOP
    print("3. Querying TMDB API (This may take a moment)...")
    count = 0
    
    for index, row in unique_movies.iterrows():
        count += 1
        # Ceremony year is usually the year after release (e.g., 2024 Ceremony honors 2023 films)
        release_year = row['year_ceremony'] - 1 
        
        print(f"   [{count}/{len(unique_movies)}] Processing: {row['film']}...")
        
        # Call function
        data = get_tmdb_data(row['film'], release_year)
        data['film'] = row['film'] # Keep original name as key
        enriched_data.append(data)
        
        time.sleep(0.15) # Rate limiting to be polite to the API

    # 4. EXPORT
    print("4. Generating final files...")
    df_enrichment = pd.DataFrame(enriched_data)

    # Merge new data with unique movies list
    df_final_movies = pd.merge(unique_movies, df_enrichment, on='film', how='left')

    # Export DIMENSION TABLE (Movie attributes)
    df_final_movies.to_csv(OUTPUT_MOVIES, index=False, encoding='utf-8')

    # Export FACT TABLE (Awards events)
    # It joins the TMDB ID back to the filtered awards table
    df_facts = pd.merge(df_filtered, df_enrichment[['film', 'tmdb_id']], on='film', how='left')
    df_facts.to_csv(OUTPUT_AWARDS, index=False, encoding='utf-8')

    print("\n--- PROCESS COMPLETED SUCCESSFULLY ---")
    print(f"Files generated:\n 1. {OUTPUT_MOVIES} (For Dim_Movie)\n 2. {OUTPUT_AWARDS} (For Fact_Award)")