**SQLite Editor v0.7 f**

**"SQLite Based Backend Platform -- from MS-Access transfer to Python"**

**1. Why this project exists -- design concept**

Microsoft Access is a rapid-application platform that bundles:

- a file based database engine (ACE / Jet),

- visual table & query designers,

- forms/reports and

- VBA for business logic.

What most Access solutions really need, however, is only the data layer
-- the tables, queries and very light-weight import/export or
printing.  
SQLiteEditorApp_V07f.py intentionally limits its scope to that *backend*
part and replaces it with:

| **Layer**       | **MS Access**             | **This project**                            |
|-----------------|---------------------------|---------------------------------------------|
| Storage engine  | ACE / Jet (\*.accdb)      | SQLite 3 (\*.db / \*.sqlite)                |
| Table designer  | "Design View"             | Schema Editor (PyQt6 tree) **or** raw SQL   |
| Data sheet view | "Datasheet View"          | Data Browser tab                            |
| Import / Export | "External data" wizards   | CSV Import / Export (threaded)              |
| Printing tables | Access Reports            | HTML based table report → preview / printer |
| Queries         | "Query Designer" SQL view | Query Editor tab                            |

The GUI is written in **PyQt 6** (pure Python, cross-platform).  
Internally we separate responsibilities:

- **DatabaseManager** -- singleton wrapper around a single SQLite
  connection (handles WAL, busy-timeout, metadata cache).

- **SchemaEditor** -- interactive designer that builds a *valid* CREATE
  TABLE statement, including PK, FK, UNIQUE, CHECK...

- **DataBrowser** -- pageable grid with sorting, per-column filtering,
  optimistic editing & batch saving.

- **ImportExportTab** -- threaded CSV import & export with progress
  dialog (cancellable).

- **QueryEditor** -- free SQL console with syntax colouring and result
  pane.

- **TableReport** -- converts the current QTableWidget page into minimal
  HTML and sends it to Qt's print framework.

The entire program is **one file** -- handy for first contact, trivial
to distribute, but easy to split later (MVC).

**2. MS Access vs. SQLite Editor -- pros & cons**

| **Topic**                 | **MS Access (ACE)**                                                          | **SQLite Editor v0.7f**                                                                                 |
|---------------------------|------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| License & cost            | Proprietary, Office license                                                  | MIT-style (Python & Qt licence). Free.                                                                  |
| Platforms                 | Windows only                                                                 | Windows, macOS, Linux, even Raspberry Pi                                                                |
| File size                 | .accdb often large because of OLE, compression only after "Compact & Repair" | .db typically smaller; WAL keeps file healthy automatically                                             |
| Concurrency (single file) | Effective only for \< 10 users; record locking; prone to corruption over VPN | WAL mode → thousands of concurrent *readers*, one writer; robust over network shares (still file-based) |
| Server mode               | Requires upsizing to SQL-Server for real multi-user                          | Same file → or move to server (SQLite over SMB)                                                         |
| Table design              | Excellent visual designer                                                    | Schema Editor close, still missing some wizards (relationships diagram)                                 |
| Forms / reports           | Built-in designer                                                            | **Not covered** -- must be replaced by PyQt, web-front-end or others                                    |
| VBA code                  | Powerful but proprietary                                                     | Switch to Python eco-system                                                                             |
| AutoNumber PK             | Native                                                                       | Checkbox "AN" (INTEGER PK AUTOINCREMENT)                                                                |
| Foreign keys              | Hidden (since ACE 2007)                                                      | Explicit FK with ON DELETE / ON UPDATE rule                                                             |
| Backup                    | Manual "Save As", Compact                                                    | SQLite is a single file -- copy or sqlite3 .backup                                                      |
| Multi-platform deploy     | No                                                                           | Portable executable or venv                                                                             |

**Summary**

- **Single user**: both are fine; SQLite files are smaller,
  zero-install, cross-platform.

- **Small work-group**: WAL gives SQLite an edge; still, Access offers
  forms/reports in the same file.

- **\>20 concurrent writers**: neither solution is ideal -- use a
  client/server RDBMS (PostgreSQL, MariaDB, SQL Server).

- **Migration path**: you can keep Access front-end and link tables to
  the new SQLite file via ODBC -- but this app is intended as a *full*
  Python replacement.

**3. Getting started -- installation**

Bash

\# 1. Install Python 3.10+ first

python -m pip install \--upgrade pip

pip install pyqt6 pyqt6-qt6 pyqt6-sip

\# optional: create a venv

python -m venv venv

venv\Scripts\activate \# Windows

source venv/bin/activate \# mac / linux

Download SQLiteEditorApp_V07f.py and run:

bash

python SQLiteEditorApp_V07f.py

**4. Operating the program -- step-by-step**

**4.1 Create / open a database**

1.  File → **Create Database** → choose a filename (\*.db).  
    The app connects immediately.

2.  File → **Open Database** to reconnect an existing file.

3.  File → **Close Database** releases the connection (Schema/Data tabs
    are cleared).

**4.2 Design tables with the Schema Editor**

1.  **Table:** pick *Select Table* → click **New Table**.

2.  Enter field properties (name, type, length).

    - Checkboxes:

      - NN = NOT NULL

      - PK = PRIMARY KEY

      - AN = AUTOINCREMENT (INTEGER only)

      - U = UNIQUE

      - FK = FOREIGN KEY (after NN matches another table's PK)

3.  Click **Add Field** -- the field appears in the grid below.

4.  Repeat for all columns.

5.  The lower text box always shows the *live* CREATE TABLE SQL.

6.  Click **Apply Changes** to create / alter the table.

**4.3 Browse & edit data**

1.  Switch to **Data Browser** tab.

2.  Select a table.

    - First page loads automatically (default page size = 1000,
      adjustable).

3.  **Filtering**: a text box appears per column -- type partial text →
    live filter.

4.  **Sorting**: click a column header to toggle ASC/DESC.

5.  **Pagination**: use *Previous* / *Next* buttons.

6.  **Editing**: type directly in a cell -- unsaved changes are shown in
    the status bar.

7.  **Add Row**: inserts a blank row (PK auto-filled if AN).

8.  **Remove Row**: select row → Remove → Save Changes.

9.  **Save Changes**: commits a batch; long batches run in a worker
    thread with progress.

10. **Print / Preview**: prints *current page* as a simple grid.

**4.4 CSV Import / Export**

1.  **Import/Export** tab → choose table.

2.  **Export to CSV** → pick a filename → all rows + header saved
    (UTF-8).

3.  **Import CSV** → pick file.

    - Header must match table columns (case-sensitive).

    - Data is inserted in 1 000-row batches -- progress dialog can be
      cancelled.

**4.5 Run arbitrary SQL**

1.  **Query Editor** tab.

2.  Type an SQL statement -- incomplete syntax is shown in red tooltip.

3.  Click **Execute Query**.

    - SELECT results are listed line-by-line.

    - DDL/DML shows "Query executed successfully".

4.  Example -- create table via SQL instead of Schema Editor:

sql

CREATE TABLE \"Products\" (

\"ProductID\" INTEGER PRIMARY KEY AUTOINCREMENT,

\"Name\" TEXT NOT NULL,

\"Price\" REAL CHECK (Price \>= 0),

\"CategoryID\" INTEGER,

FOREIGN KEY(\"CategoryID\") REFERENCES \"Categories\"(\"CategoryID\") ON
DELETE SET NULL

);
