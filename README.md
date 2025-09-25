# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project analyzes user listening behavior and music trends from a fictional music streaming platform. Using Apache Spark's Structured APIs (DataFrames), we process and transform structured data to gain insights into genre preferences, song popularity, and listener engagement.  

The analysis is performed on two datasets: user listening logs and song metadata.

---

## Dataset Description
The analysis uses two main CSV datasets:

### 1. `listening_logs.csv`
Contains records of user listening activity.

| Column        | Description                                   |
|---------------|-----------------------------------------------|
| user_id       | Unique identifier for the user               |
| song_id       | Unique identifier for the song               |
| timestamp     | Date and time the song was played            |
| duration_sec  | Duration (in seconds) for which the song was played |

### 2. `songs_metadata.csv`
Contains metadata for each song.

| Column   | Description                                         |
|----------|-----------------------------------------------------|
| song_id  | Unique identifier for the song                     |
| title    | Title of the song                                  |
| artist   | Name of the artist                                 |
| genre    | Genre of the song (e.g., Pop, Rock, Jazz)        |
| mood     | Mood category of the song (e.g., Happy, Sad)     |

---


## Repository Structure
├── listening_logs.csv
├── songs_metadata.csv
├── main.py
├── output/
└── README.md

---

## Output Directory Structure
The results from the analysis are stored in the `output/` directory, with each task's output saved in a dedicated subdirectory.

output/
├── user_favorite_genres/
│ └── part-00000-....csv
├── avg_listen_time_per_song/
│ └── part-00000-....csv
├── genre_loyalty_scores/
│ └── part-00000-....csv
└── night_owl_users/
└── part-00000-....csv


---

## Tasks and Outputs

### 1. Find Each User's Favourite Genre
- **Objective:** Identify the most listened-to genre for each user.  
- **Approach:** Join listening logs with song metadata, group by `user_id` and `genre`, count occurrences, and use a Spark Window function to rank genres per user.  
- **Output:** `output/user_favorite_genres/`

### 2. Calculate the Average Listen Time per Song
- **Objective:** Compute the average duration each song was played.  
- **Approach:** Group `listening_logs` by `song_id` and calculate the average of `duration_sec`.  
- **Output:** `output/avg_listen_time_per_song/`

### 3. Compute the Genre Loyalty Score
- **Objective:** For each user, calculate the proportion of plays that belong to their most-listened genre and identify users with a loyalty score > 0.8.  
- **Approach:** Calculate total plays per user, join with favorite genre play count, compute loyalty score, and filter users with a score > 0.8.  
- **Output:** `output/genre_loyalty_scores/`

### 4. Identify Night Owl Users
- **Objective:** Find users who frequently listen to music between 12 AM and 5 AM.  
- **Approach:** Extract hour from `timestamp` and filter for hours 0–4, then select distinct user IDs.  
- **Output:** `output/night_owl_users/`

---

## Execution Instructions

### Prerequisites
While the assignment can be run locally, the recommended approach is to use **GitHub Codespaces**, which comes pre-configured with Spark and Python.

If running locally, ensure you have:

- **Python 3.x**  
  ```bash
  python3 --version

## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions
**Error:**  
org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory ... already exists

**Cause:**  
This error occurs if you run the `spark-submit` command more than once, because Spark does not overwrite an existing output directory by default.

**Resolution:**  
The `main.py` script is already configured with `.mode("overwrite")` for each write operation. This ensures that any existing output directories are automatically overwritten, preventing this error.  


