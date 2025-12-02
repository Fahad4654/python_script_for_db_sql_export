## 💾 python\_script\_for\_db\_sql\_export

This repository contains a **Python script** designed to facilitate the **export of a PostgreSQL database schema and data to a single SQL file**. This is useful for backup, migration, or development purposes.

-----

### 📋 Requirements

The following system components and software versions are required to successfully run this script:

  * **Operating System: Ubuntu** (or a compatible Linux distribution)
  * **Database: PostgreSQL** (configured and accessible)
  * **Programming Language: Python>= 3.11**

-----

### 🚀 Getting Started

Follow these steps to set up and execute the database export script.

#### 1\. Clone the Repository

Clone the project repository from GitHub:

```bash
git clone https://github.com/Fahad4654/python_script_for_db_sql_export.git
```

#### 2\. Navigate to the Directory

Change into the newly created project directory:

```bash
cd python_script_for_db_sql_export
```

#### 3\. Set Up Virtual Environment

Create a Python **virtual environment** to manage project dependencies:

```bash
python -m venv .venv
```

#### 4\. Activate Virtual Environment

Activate the virtual environment. This ensures that the required Python libraries are installed and executed in an isolated environment:

```bash
source .venv/bin/activate
```

#### 5\. Install Dependencies

Install the necessary Python packages listed in the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

#### 6\. Configure Environment Variables

Copy the example environment file to create your local configuration file:

```bash
cp env.example .env
```

#### 7\. Update Configuration

Open the **`.env`** file and **replace the placeholder environment variables** with your specific **PostgreSQL database connection settings**.

#### 8\. Execute the Script

Run the main Python script to perform the database export:

```bash
python main.py
```

The exported **SQL** file will be generated in the **`./result/`** directory.

-----


## 📂 Export File Location

After running `python main.py`, your exported files will be found here:

```
python_script_for_db_sql_export/result/
```

The full path relative to your current working directory after Step 2 (`cd python_script_for_db_sql_export`) will be:

```bash
./result/
```