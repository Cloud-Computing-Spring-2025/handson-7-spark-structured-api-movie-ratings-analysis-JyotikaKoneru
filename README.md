# Movie Ratings Analysis

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
workspaces/handson-7-spark-structured-api-movie-ratings-analysis-JyotikaKoneru/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├── churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── docker-compose.yml
└── README.md
```

- **input/**: Contains the `movie_ratings_data.csv` dataset.
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **docker-compose.yml**: Docker Compose configuration file to set up Spark.
- **README.md**: Assignment instructions and guidelines.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd workspaces/handson-7-spark-structured-api-movie-ratings-analysis-JyotikaKoneru/
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```
   You should see:
   - `binge_watching_patterns.csv`
   - `churn_risk_users.csv`
   - `movie_watching_trends.csv`

#### **b. Running with Docker**
If using Docker, ensure that your Docker setup is correct, then execute:
   ```bash
   docker-compose up
   ```

## **Dataset Overview**

### **Dataset: Advanced Movie Ratings & Streaming Trends**

You will work with a dataset containing information about **100+ users** who rated movies across various streaming platforms. The dataset includes the following columns:

| **Column Name**         | **Data Type**  | **Description** |
|-------------------------|---------------|----------------|
| **UserID**             | Integer       | Unique identifier for a user |
| **MovieID**            | Integer       | Unique identifier for a movie |
| **MovieTitle**         | String        | Name of the movie |
| **Genre**             | String        | Movie genre (e.g., Action, Comedy, Drama) |
| **Rating**            | Float         | User rating (1.0 to 5.0) |
| **ReviewCount**       | Integer       | Total reviews given by the user |
| **WatchedYear**       | Integer       | Year when the movie was watched |
| **UserLocation**      | String        | User's country |
| **AgeGroup**          | String        | Age category (Teen, Adult, Senior) |
| **StreamingPlatform** | String        | Platform where the movie was watched |
| **WatchTime**        | Integer       | Total watch time in minutes |
| **IsBingeWatched**    | Boolean       | True if the user watched 3+ movies in a day |
| **SubscriptionStatus** | String        | Subscription status (Active, Canceled) |

## **Analysis Results**

### **1. Detect Binge-Watching Patterns**

**Output:**

| Age Group | Binge Watchers | Total Users | Percentage |
|-----------|---------------|-------------|------------|
| Senior    | 12            | 32          | 37.5%      |
| Teen      | 17            | 29          | 58.62%     |
| Adult     | 23            | 39          | 58.97%     |

---

### **2. Identify Churn Risk Users**

**Output:**

| Churn Risk Users                                  | Total Users |
|--------------------------------------------------|-------------|
| Users with low watch time & canceled subscriptions | 8           |

---

### **3. Trend Analysis Over the Years**

**Output:**

| Watched Year | Movies Watched |
|-------------|----------------|
| 2018        | 22             |
| 2019        | 13             |
| 2020        | 13             |
| 2021        | 20             |
| 2022        | 20             |
| 2023        | 12             |

## **Conclusion**

This project provided valuable insights into movie-watching behaviors using Spark Structured APIs. By analyzing binge-watching trends, identifying churn risk users, and examining yearly viewing trends, we successfully demonstrated how big data processing techniques can be leveraged for business intelligence. The findings can help streaming platforms optimize their user engagement strategies and enhance customer retention. Further enhancements could include sentiment analysis of user reviews or a recommendation engine based on viewing history.

