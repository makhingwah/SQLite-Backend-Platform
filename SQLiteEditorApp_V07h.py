import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTabWidget,
                            QLabel, QPushButton, QFileDialog, QMessageBox, QComboBox, QLineEdit,
                            QCheckBox, QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
                            QInputDialog, QTextEdit, QHeaderView, QSpinBox, QProgressDialog)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QAction, QTextCursor, QTextCharFormat, QColor
from PyQt6.QtPrintSupport import QPrinter, QPrintPreviewDialog, QPrintDialog
from PyQt6.QtGui import QTextDocument
import sqlite3
import csv
import os
import logging
import time

logging.basicConfig(level=logging.DEBUG, filename='SQLite_Edit.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# ----------------------
# TableReport (HTML-based report for QPrinter)
# ----------------------
class TableReport:
    """Build a simple HTML table from a QTableWidget page and send it to either a QPrintPreviewDialog or to an actual printer."""
    def __init__(self, qtable, title="Table Report"):
        self.table = qtable
        self.title = title

    def preview(self, parent=None):
        if not self._is_valid_table():
            QMessageBox.warning(parent, "Warning", "No data available for preview.")
            return
        printer = QPrinter(QPrinter.PrinterMode.HighResolution)
        preview = QPrintPreviewDialog(printer, parent)
        preview.setWindowTitle(f"Preview – {self.title}")
        preview.paintRequested.connect(lambda p: self._render(p))
        preview.exec()

    def print_(self, parent=None):
        if not self._is_valid_table():
            QMessageBox.warning(parent, "Warning", "No data available for printing.")
            return
        printer = QPrinter(QPrinter.PrinterMode.HighResolution)
        dlg = QPrintDialog(printer, parent)
        dlg.setWindowTitle(f"Print – {self.title}")
        if dlg.exec() == QPrintDialog.DialogCode.Accepted:
            self._render(printer)

    def _render(self, printer):
        doc = QTextDocument()
        html = self._build_html()
        doc.setHtml(html)
        doc.print(printer)

    def _build_html(self):
        row_cnt = self.table.rowCount()
        col_cnt = self.table.columnCount()
        if row_cnt == 0 or col_cnt == 0:
            return "No Data\n"
        header_cells = "".join(
            f"	{self.table.horizontalHeaderItem(c).text()}\n"
            for c in range(col_cnt) if self.table.horizontalHeaderItem(c)
        )
        body_rows = []
        for r in range(row_cnt):
            cells = []
            for c in range(col_cnt):
                item = self.table.item(r, c)
                cells.append(f"	{item.text() if item else ''}\n")
            body_rows.append(f"\n{''.join(cells)}")
        body_html = "\n".join(body_rows)
        return f"""
\n\n
    {self.title}\n\n
    \n\n\n
                {header_cells}
            \n
        \n\n
            {body_html}
        \n
    \n
"""

    def _is_valid_table(self):
        return (self.table and self.table.columnCount() > 0 and
                self.table.rowCount() > 0 and
                all(self.table.horizontalHeaderItem(c) for c in range(self.table.columnCount())))

# ----------------------
# DatabaseManager
# ----------------------
class DatabaseManager:
    """Singleton class to manage SQLite database connections."""
    _instance = None

    @staticmethod
    def get_instance():
        if not DatabaseManager._instance:
            DatabaseManager._instance = DatabaseManager()
        return DatabaseManager._instance

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.db_path = None
        self.metadata_cache = {}

    def connect(self, file_path):
        try:
            if self.conn:
                self.close()
            if not os.path.exists(file_path):
                open(file_path, 'a').close()
            self.conn = sqlite3.connect(file_path)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA locking_mode=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=8000")
            self.cursor = self.conn.cursor()
            self.db_path = file_path
            self.metadata_cache.clear()
            logging.info(f"Connected to database: {file_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Failed to connect to database: {str(e)}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            self.db_path = None
            self.metadata_cache.clear()
            logging.info("Database connection closed")

    def is_connected(self):
        return self.conn is not None

    def get_tables(self):
        if self.is_connected():
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0].strip() for row in self.cursor.fetchall()]
            logging.info(f"Retrieved tables: {tables}")
            return tables
        return []

    def get_table_info(self, table_name):
        if table_name not in self.metadata_cache:
            self.cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = self.cursor.fetchall()
            self.cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
            fks = self.cursor.fetchall()
            self.metadata_cache[table_name] = {"columns": columns, "foreign_keys": fks}
            logging.debug(f"Table info for {table_name}: columns={columns}, foreign_keys={fks}")
        return self.metadata_cache[table_name]

    def backup_table(self, old_name, new_name):
        self.cursor.execute(f'CREATE TABLE "{new_name}" AS SELECT * FROM "{old_name}"')
        logging.info(f"Backed up {old_name} to {new_name}")

    def execute_query(self, query, params=(), retries=3, delay=1):
        for attempt in range(retries):
            try:
                self.cursor.execute(query, params)
                self.conn.commit()
                return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    if attempt < retries - 1:
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                        continue
                    raise RuntimeError("The database is busy, please retry in a moment.") from e
                raise

# ----------------------
# SchemaEditor
# ----------------------
class SchemaEditor(QWidget):
    """Widget for creating and modifying table schemas."""
    table_changed = pyqtSignal()  # Signal to notify table changes

    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.selected_field = None
        self.original_fields = []
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        table_layout = QHBoxLayout()
        table_layout.addWidget(QLabel("Table:"))
        self.table_combo = QComboBox()
        self.table_combo.currentTextChanged.connect(self.load_table)
        table_layout.addWidget(self.table_combo)
        new_btn = QPushButton("New Table")
        new_btn.clicked.connect(self.new_table)
        table_layout.addWidget(new_btn)
        layout.addLayout(table_layout)

        field_layout = QVBoxLayout()
        input_row1 = QHBoxLayout()
        self.field_name = QLineEdit()
        self.field_name.setPlaceholderText("Field Name (e.g., Staff ID)")
        self.field_name.setMaximumWidth(400)
        self.field_name.textChanged.connect(self.update_fk_check_state)
        input_row1.addWidget(self.field_name)
        self.field_type = QComboBox()
        self.field_type.addItems(["INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC", "DATE", "BOOLEAN", "CHAR", "VARCHAR"])
        self.field_type.setMinimumWidth(75)
        self.field_type.setCurrentText("INTEGER")
        self.field_type.currentTextChanged.connect(self.toggle_length_input)
        self.field_type.currentTextChanged.connect(self.update_fk_check_state)
        input_row1.addWidget(self.field_type)
        self.length_input = QLineEdit()
        self.length_input.setPlaceholderText("Length (e.g., 50)")
        self.length_input.setMaximumWidth(100)
        self.length_input.setVisible(False)
        input_row1.addWidget(self.length_input)
        field_layout.addLayout(input_row1)

        input_row2 = QHBoxLayout()
        self.not_null = QCheckBox("NN")
        self.not_null.setToolTip("Not Null")
        input_row2.addWidget(self.not_null)
        self.primary_key = QCheckBox("PK")
        self.primary_key.setToolTip("Primary Key")
        input_row2.addWidget(self.primary_key)
        self.auto_number = QCheckBox("AN")
        self.auto_number.setToolTip("AutoNumber")
        input_row2.addWidget(self.auto_number)
        self.unique = QCheckBox("U")
        self.unique.setToolTip("Unique")
        input_row2.addWidget(self.unique)
        field_layout.addLayout(input_row2)

        input_row3 = QHBoxLayout()
        self.default_value = QLineEdit()
        self.default_value.setPlaceholderText("Default Value (e.g., 0)")
        input_row3.addWidget(self.default_value)
        self.check_constraint = QLineEdit()
        self.check_constraint.setPlaceholderText("Check (e.g., age > 18)")
        input_row3.addWidget(self.check_constraint)
        field_layout.addLayout(input_row3)

        input_row4 = QHBoxLayout()
        self.foreign_key = QCheckBox("FK")
        self.foreign_key.setToolTip("Foreign Key")
        self.foreign_key.stateChanged.connect(self.update_fk_widgets)
        input_row4.addWidget(self.foreign_key)
        self.fk_on_delete = QComboBox()
        self.fk_on_delete.addItems(["NO ACTION", "CASCADE", "SET NULL", "RESTRICT"])
        self.fk_on_delete.setEnabled(False)
        self.fk_on_delete.currentTextChanged.connect(self.update_schema)
        input_row4.addWidget(QLabel("ON DELETE:"))
        input_row4.addWidget(self.fk_on_delete)
        self.fk_on_update = QComboBox()
        self.fk_on_update.addItems(["NO ACTION", "CASCADE", "SET NULL", "RESTRICT"])
        self.fk_on_update.setEnabled(False)
        self.fk_on_update.currentTextChanged.connect(self.update_schema)
        input_row4.addWidget(QLabel("ON UPDATE:"))
        input_row4.addWidget(self.fk_on_update)
        field_layout.addLayout(input_row4)

        input_row5 = QHBoxLayout()
        self.fk_table = QComboBox()
        self.fk_table.addItem("Select Table")
        self.fk_table.setEnabled(False)
        self.fk_table.currentTextChanged.connect(self.update_fk_column_combo)
        input_row5.addWidget(self.fk_table)
        self.fk_column = QComboBox()
        self.fk_column.setEnabled(False)
        input_row5.addWidget(self.fk_column)
        field_layout.addLayout(input_row5)

        button_row = QHBoxLayout()
        self.add_field_btn = QPushButton("Add Field")
        self.add_field_btn.clicked.connect(self.add_field)
        button_row.addWidget(self.add_field_btn)
        self.modify_field_btn = QPushButton("Modify Field")
        self.modify_field_btn.setEnabled(False)
        self.modify_field_btn.clicked.connect(self.modify_field)
        button_row.addWidget(self.modify_field_btn)
        self.remove_field_btn = QPushButton("Remove Row")
        self.remove_field_btn.setEnabled(False)
        self.remove_field_btn.clicked.connect(self.remove_field)
        button_row.addWidget(self.remove_field_btn)
        field_layout.addLayout(button_row)
        layout.addLayout(field_layout)

        self.field_tree = QTreeWidget()
        self.field_tree.setHeaderLabels(["Name", "Type", "Length", "NN", "PK", "AN", "U", "Default", "Check", "FK"])
        self.field_tree.setColumnWidth(0, 150)
        self.field_tree.setColumnWidth(1, 100)
        self.field_tree.itemClicked.connect(self.select_field)
        layout.addWidget(self.field_tree)

        self.schema_box = QTextEdit()
        self.schema_box.setReadOnly(True)
        self.schema_box.setFixedHeight(100)
        self.schema_box.setPlaceholderText("CREATE TABLE statement will appear here")
        layout.addWidget(self.schema_box)

        apply_btn = QPushButton("Apply Changes")
        apply_btn.clicked.connect(self.apply_changes)
        layout.addWidget(apply_btn)

        self.fields = []
        self.refresh_tables()

    def refresh_tables(self):
        self.table_combo.clear()
        self.table_combo.addItem("Select Table")
        if self.db_manager.is_connected():
            self.table_combo.addItems(self.db_manager.get_tables())
        self.clear_fields(preserve_fields=False)
        self.original_fields = self.fields.copy()

    def new_table(self):
        table_name, ok = QInputDialog.getText(self, "New Table", "Enter table name:")
        if ok and table_name:
            if table_name.strip() in self.db_manager.get_tables():
                QMessageBox.critical(self, "Error", "Table already exists")
                return
            if not table_name.strip() or any(c in table_name for c in ";\"'"):
                QMessageBox.critical(self, "Error", "Invalid table name")
                return
            self.table_combo.addItem(table_name.strip())
            self.table_combo.setCurrentText(table_name.strip())
            self.clear_fields(preserve_fields=False)
            self.original_fields = self.fields.copy()

    def clear_fields(self, preserve_fields=True):
        if not preserve_fields:
            self.field_tree.clear()
            self.fields = []
        self.field_name.clear()
        self.field_type.setCurrentText("INTEGER")
        self.length_input.clear()
        self.length_input.setVisible(False)
        self.not_null.setChecked(False)
        self.primary_key.setChecked(False)
        self.auto_number.setChecked(False)
        self.auto_number.setEnabled(False)
        self.unique.setChecked(False)
        self.foreign_key.setChecked(False)
        self.foreign_key.setEnabled(False)
        self.fk_table.setEnabled(False)
        self.fk_column.clear()
        self.fk_column.setEnabled(False)
        self.fk_on_delete.setEnabled(False)
        self.fk_on_update.setEnabled(False)
        self.default_value.clear()
        self.check_constraint.clear()
        self.fk_table.setCurrentIndex(0)
        self.modify_field_btn.setEnabled(False)
        self.remove_field_btn.setEnabled(False)
        self.selected_field = None
        logging.debug(
            f"Cleared fields, UI state: modify_btn={self.modify_field_btn.isEnabled()}, remove_btn={self.remove_field_btn.isEnabled()}")

    def toggle_length_input(self, type_name):
        is_char_type = type_name in ["CHAR", "VARCHAR"]
        self.length_input.setVisible(is_char_type)
        logging.debug(f"Toggled length input for type {type_name}, visibility={is_char_type}")
        self.update_schema()

    def update_fk_check_state(self):
        current_table = self.table_combo.currentText().strip()
        field_name = self.field_name.text().strip()
        field_type = self.get_current_field_type()
        can_be_fk = False

        if field_name and field_type and current_table != "Select Table":
            tables = [t for t in self.db_manager.get_tables() if t != current_table]
            logging.debug(
                f"update_fk_check_state: Checking field_name={field_name}, field_type={field_type}, current_table={current_table}, tables={tables}")
            for table in tables:
                info = self.db_manager.get_table_info(table)
                pk_fields = [col for col in info["columns"] if col[5]]  # col[5] is pk
                if len(pk_fields) == 1:
                    pk_id, pk_name, pk_type, _, _, _ = pk_fields[0]
                    pk_base_type = pk_type.split("(")[0] if "(" in pk_type else pk_type
                    field_base_type = field_type.split("(")[0] if "(" in field_type else field_type
                    logging.debug(
                        f"Comparing: pk_name={pk_name}, pk_type={pk_type}, pk_base_type={pk_base_type}, field_name={field_name}, field_type={field_type}, field_base_type={field_base_type}")
                    if pk_base_type == field_base_type:  # Only type compatibility required
                        can_be_fk = True
                        break

        self.foreign_key.setEnabled(can_be_fk)
        logging.debug(f"update_fk_check_state: FK enabled={can_be_fk}")

    def get_current_field_type(self):
        type_name = self.field_type.currentText()
        length = self.length_input.text().strip() if self.length_input.isVisible() and self.length_input.text().strip() else ""
        if type_name in ["CHAR", "VARCHAR"] and length:
            return f"{type_name}({length})"
        return type_name

    def update_fk_widgets(self):
        enabled = self.foreign_key.isChecked()
        self.fk_table.setEnabled(enabled)
        self.fk_column.setEnabled(enabled)
        self.fk_on_delete.setEnabled(enabled)
        self.fk_on_update.setEnabled(enabled)
        if enabled:
            self.update_fk_ref_table_combo()
        else:
            self.fk_table.setCurrentIndex(0)
            self.fk_column.clear()
            self.fk_on_delete.setCurrentText("RESTRICT")
            self.fk_on_update.setCurrentText("RESTRICT")
        if self.selected_field:
            self.update_field_properties()
        self.update_schema()
        logging.debug(f"update_fk_widgets: enabled={enabled}")

    def update_fk_ref_table_combo(self):
        self.fk_table.blockSignals(True)
        self.fk_table.clear()
        self.fk_table.addItem("Select Table")
        field_name = self.field_name.text().strip()
        field_type = self.get_current_field_type()
        current_table = self.table_combo.currentText().strip()

        if field_name and field_type and current_table != "Select Table":
            tables = [t for t in self.db_manager.get_tables() if t != current_table]
            for table in tables:
                info = self.db_manager.get_table_info(table)
                pk_fields = [col for col in info["columns"] if col[5]]  # col[5] is pk
                if len(pk_fields) == 1:
                    pk_id, pk_name, pk_type, _, _, _ = pk_fields[0]
                    pk_base_type = pk_type.split("(")[0] if "(" in pk_type else pk_type
                    field_base_type = field_type.split("(")[0] if "(" in field_type else field_type
                    if pk_base_type == field_base_type:
                        self.fk_table.addItem(table)
        self.fk_table.blockSignals(False)
        self.update_fk_column_combo()
        logging.debug(f"update_fk_ref_table_combo: tables added={self.fk_table.count() - 1}")

    def update_fk_column_combo(self):
        self.fk_column.blockSignals(True)
        self.fk_column.clear()
        ref_table = self.fk_table.currentText()
        if ref_table and ref_table != "Select Table":
            info = self.db_manager.get_table_info(ref_table)
            pk_fields = [col for col in info["columns"] if col[5]]  # col[5] is pk
            if len(pk_fields) == 1:
                _, pk_name, _, _, _, _ = pk_fields[0]
                self.fk_column.addItem(pk_name)
        self.fk_column.blockSignals(False)
        logging.debug(f"update_fk_column_combo: columns added={self.fk_column.count()}")

    def update_auto_number(self):
        if self.auto_number.isChecked() and not self.primary_key.isChecked():
            self.primary_key.setChecked(True)
        if self.selected_field:
            self.update_field_properties()
        logging.debug(f"Updated auto number, state={self.auto_number.isChecked()}")
        self.update_schema()

    def get_table_sql(self, table_name):
        try:
            self.db_manager.cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
            )
            result = self.db_manager.cursor.fetchone()
            return result[0] if result else ""
        except sqlite3.Error as e:
            logging.error(f"Failed to get table SQL for {table_name}: {str(e)}")
            return ""

    def load_table(self, table_name):
        self.clear_fields(preserve_fields=False)
        if not table_name or table_name == "Select Table":
            return
        try:
            info = self.db_manager.get_table_info(table_name)
            create_sql = self.get_table_sql(table_name).upper()
            for col in info["columns"]:
                cid, name, col_type, not_null, default, pk = col
                is_auto = col_type == "INTEGER" and pk and "AUTOINCREMENT" in create_sql
                is_unique = f'"{name}"' in create_sql and "UNIQUE" in create_sql and not pk
                fk_info = next((fk for fk in info.get("foreign_keys", []) if fk[3] == name), None)
                fk_display = f"{fk_info[2]}({fk_info[4]})" if fk_info else ""
                item = QTreeWidgetItem([
                    name, col_type,
                    col_type[col_type.find("(") + 1:col_type.find(")")] if "(" in col_type else "",
                    "✓" if not_null else "",
                    "✓" if pk else "",
                    "✓" if is_auto else "",
                    "✓" if is_unique else "",
                    str(default) if default else "",
                    "", fk_display
                ])
                self.field_tree.addTopLevelItem(item)
                self.fields.append({
                    "name": name, "type": col_type, "not_null": bool(not_null), "primary_key": bool(pk),
                    "auto_number": is_auto, "unique": is_unique, "default": default, "check": "",
                    "foreign_key": {"table": fk_info[2] if fk_info else "", "column": fk_info[4] if fk_info else "",
                                    "on_delete": fk_info[5] if fk_info else "RESTRICT",
                                    "on_update": fk_info[6] if fk_info else "RESTRICT"}
                })
            self.original_fields = self.fields.copy()
            logging.debug(f"Loaded table {table_name}, fields={self.fields}")
            self.update_schema()
            self.update_fk_check_state()
        except Exception as e:
            logging.error(f"Failed to load table {table_name}: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to load table: {str(e)}")

    def select_field(self, item, column):
        self.selected_field = item
        self.update_field_properties()
        self.modify_field_btn.setEnabled(True)
        self.remove_field_btn.setEnabled(True)
        logging.debug(f"Selected field: {item.text(0)}")

    def update_field_properties(self):
        if not self.selected_field:
            self.modify_field_btn.setEnabled(False)
            self.remove_field_btn.setEnabled(False)
            return
        field_data = next((f for f in self.fields if f["name"] == self.selected_field.text(0)), None)
        if not field_data:
            self.modify_field_btn.setEnabled(False)
            self.remove_field_btn.setEnabled(False)
            return
        self.field_name.setText(field_data["name"])
        type_text = field_data["type"]
        if "(" in type_text:
            base_type, length = type_text.split("(")
            length = length.rstrip(")")
            self.field_type.setCurrentText(base_type)
            self.length_input.setText(length)
        else:
            self.field_type.setCurrentText(type_text)
            self.length_input.clear()
        self.toggle_length_input(self.field_type.currentText())
        self.not_null.setChecked(field_data["not_null"])
        self.primary_key.setChecked(field_data["primary_key"])
        self.auto_number.setChecked(field_data["auto_number"])
        self.unique.setChecked(field_data["unique"])
        self.foreign_key.setChecked(bool(field_data["foreign_key"]["table"]))
        self.default_value.setText(str(field_data["default"]) if field_data["default"] else "")
        self.check_constraint.setText(field_data["check"])
        self.fk_table.setCurrentText(field_data["foreign_key"]["table"] or "Select Table")
        self.fk_column.setCurrentText(field_data["foreign_key"]["column"] or "")
        self.fk_on_delete.setCurrentText(field_data["foreign_key"]["on_delete"])
        self.fk_on_update.setCurrentText(field_data["foreign_key"]["on_update"])
        self.fk_table.setEnabled(bool(field_data["foreign_key"]["table"]))
        self.fk_column.setEnabled(bool(field_data["foreign_key"]["table"]))
        self.fk_on_delete.setEnabled(bool(field_data["foreign_key"]["table"]))
        self.fk_on_update.setEnabled(bool(field_data["foreign_key"]["table"]))
        self.update_fk_check_state()

    def add_field(self):
        name = self.field_name.text().strip()
        if not name:
            QMessageBox.critical(self, "Error", "Field name cannot be empty")
            return
        if name in [f["name"] for f in self.fields]:
            QMessageBox.critical(self, "Error", "Field name already exists in this table")
            return
        if self.field_type.currentIndex() == -1:
            QMessageBox.warning(self, "Warning", "Please assign a datatype to the field")
            return
        if self.auto_number.isChecked() and any(f["auto_number"] for f in self.fields):
            QMessageBox.critical(self, "Error", "Only one AutoNumber field is allowed per table")
            return
        type_name = self.get_current_field_type()
        field = {
            "name": name, "type": type_name, "not_null": self.not_null.isChecked(),
            "primary_key": self.primary_key.isChecked(), "auto_number": self.auto_number.isChecked(),
            "unique": self.unique.isChecked(), "default": self.default_value.text().strip(),
            "check": self.check_constraint.text().strip(),
            "foreign_key": {
                "table": self.fk_table.currentText() if self.foreign_key.isChecked() and self.fk_table.currentText() != "Select Table" else "",
                "column": self.fk_column.currentText() if self.fk_column.currentText() else "",
                "on_delete": self.fk_on_delete.currentText(),
                "on_update": self.fk_on_update.currentText()}
        }
        if field["auto_number"] and not field["type"].startswith("INTEGER"):
            QMessageBox.critical(self, "Error", "AutoNumber requires INTEGER type")
            return
        self.fields.append(field)
        fk_display = f"{self.fk_table.currentText()}({self.fk_column.currentText()}) ON DELETE {self.fk_on_delete.currentText()} ON UPDATE {self.fk_on_update.currentText()}" if self.foreign_key.isChecked() else ""
        item = QTreeWidgetItem([
            name, field["type"].split("(")[0] if "(" in field["type"] else field["type"],
            field["type"][field["type"].find("(") + 1:field["type"].find(")")] if "(" in field["type"] else "",
            "✓" if field["not_null"] else "", "✓" if field["primary_key"] else "",
            "✓" if field["auto_number"] else "", "✓" if field["unique"] else "",
            field["default"], field["check"], fk_display
        ])
        self.field_tree.addTopLevelItem(item)
        self.clear_fields(preserve_fields=True)
        self.update_schema()
        logging.debug(f"Added field: {field}")

    def modify_field(self):
        if not self.selected_field:
            return
        try:
            old_name = self.selected_field.text(0)
            new_name = self.field_name.text().strip()
            if not new_name:
                QMessageBox.critical(self, "Error", "Field name cannot be empty")
                return
            if new_name in [f["name"] for f in self.fields if f["name"] != old_name]:
                QMessageBox.critical(self, "Error", "Field name already exists in this table")
                return
            if self.field_type.currentIndex() == -1:
                QMessageBox.warning(self, "Warning", "Please assign a datatype to the field")
                return
            if self.auto_number.isChecked() and any(f["auto_number"] for f in self.fields if f["name"] != old_name):
                QMessageBox.critical(self, "Error", "Only one AutoNumber field is allowed per table")
                return
            new_type = self.get_current_field_type()
            field_index = next(i for i, f in enumerate(self.fields) if f["name"] == old_name)
            old_data = self.fields[field_index].copy()
            self.fields[field_index].update({
                "name": new_name,
                "type": new_type,
                "not_null": self.not_null.isChecked(),
                "primary_key": self.primary_key.isChecked(),
                "auto_number": self.auto_number.isChecked(),
                "unique": self.unique.isChecked(),
                "default": self.default_value.text().strip(),
                "check": self.check_constraint.text().strip(),
                "foreign_key": {
                    "table": self.fk_table.currentText() if self.foreign_key.isChecked() and self.fk_table.currentText() != "Select Table" else "",
                    "column": self.fk_column.currentText() if self.fk_column.currentText() else "",
                    "on_delete": self.fk_on_delete.currentText(),
                    "on_update": self.fk_on_update.currentText()}
            })
            self.selected_field.setText(0, new_name)
            self.selected_field.setText(1, new_type.split("(")[0] if "(" in new_type else new_type)
            self.selected_field.setText(2,
                                        new_type[new_type.find("(") + 1:new_type.find(")")] if "(" in new_type else "")
            self.selected_field.setText(3, "✓" if self.not_null.isChecked() else "")
            self.selected_field.setText(4, "✓" if self.primary_key.isChecked() else "")
            self.selected_field.setText(5, "✓" if self.auto_number.isChecked() else "")
            self.selected_field.setText(6, "✓" if self.unique.isChecked() else "")
            self.selected_field.setText(7,
                                        self.default_value.text().strip() if self.default_value.text().strip() else "")
            self.selected_field.setText(8,
                                        self.check_constraint.text().strip() if self.check_constraint.text().strip() else "")
            fk_display = f"{self.fk_table.currentText()}({self.fk_column.currentText()}) ON DELETE {self.fk_on_delete.currentText()} ON UPDATE {self.fk_on_update.currentText()}" if self.foreign_key.isChecked() else ""
            self.selected_field.setText(9, fk_display)
            self.update_schema()
            self.clear_fields(preserve_fields=True)
            logging.debug(f"Modified field: {new_name}, old data={old_data}, new data={self.fields[field_index]}")
        except Exception as e:
            logging.error(f"Error in modify_field: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to modify field: {str(e)}")

    def remove_field(self):
        if not self.selected_field:
            return
        name = self.selected_field.text(0)
        self.fields = [f for f in self.fields if f["name"] != name]
        self.field_tree.takeTopLevelItem(self.field_tree.indexOfTopLevelItem(self.selected_field))
        self.clear_fields(preserve_fields=True)
        self.update_schema()
        logging.debug(f"Removed field: {name}")

    def update_schema(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table" or not self.fields:
            self.schema_box.clear()
            return
        columns = []
        pk_fields = []
        fk_constraints = []
        for field in self.fields:
            col_def = f'\t"{field["name"]}"\t{field["type"]}'
            if field["not_null"]:
                col_def += " NOT NULL"
            if field["unique"] and not field["primary_key"]:
                col_def += " UNIQUE"
            if field["default"]:
                col_def += f" DEFAULT {field['default']}"
            if field["check"]:
                col_def += f" CHECK ({field['check']})"
            columns.append(col_def)
            if field["primary_key"]:
                pk_fields.append(f'"{field["name"]}"')
            if field["foreign_key"]["table"]:
                fk_constraint = f'\tFOREIGN KEY("{field["name"]}") REFERENCES "{field["foreign_key"]["table"]}"("{field["foreign_key"]["column"]}")'
                if field["foreign_key"]["on_delete"] != "RESTRICT":
                    fk_constraint += f" ON DELETE {field['foreign_key']['on_delete']}"
                if field["foreign_key"]["on_update"] != "RESTRICT":
                    fk_constraint += f" ON UPDATE {field['foreign_key']['on_update']}"
                fk_constraints.append(fk_constraint)
        sql = f'CREATE TABLE "{table_name}" (\n'
        sql += ",\n".join(columns)
        if pk_fields:
            sql += f",\n\tPRIMARY KEY({','.join(pk_fields)})"
            if any(f["auto_number"] for f in self.fields if f["primary_key"]):
                sql = sql.replace("PRIMARY KEY", "PRIMARY KEY AUTOINCREMENT")
        if fk_constraints:
            sql += ",\n" + ",\n".join(fk_constraints)
        sql += '\n);'
        self.schema_box.setText(sql)
        logging.debug(f"Updated schema for {table_name}: {sql}")

    def apply_changes(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table" or not self.fields:
            QMessageBox.critical(self, "Error", "Select a table and add fields")
            return
        for field in self.fields:
            if not field["type"]:
                QMessageBox.warning(self, "Warning",
                                    f"Field '{field['name']}' has no datatype assigned. Please assign a datatype before applying changes.")
                return
        try:
            logging.debug(f"Applying changes for table: {table_name}")
            existing_tables = self.db_manager.get_tables()
            if table_name in existing_tables:
                existing_info = self.db_manager.get_table_info(table_name)
                existing_columns = {col[1]: col for col in existing_info["columns"]}
                new_fields = [f for f in self.fields if f["name"] not in existing_columns]
                modified_fields = []
                for f in self.fields:
                    if f["name"] in existing_columns:
                        col = existing_columns[f["name"]]
                        if (f["type"] != col[2] or
                                f["not_null"] != bool(col[3]) or
                                f["default"] != col[4] or
                                f["primary_key"] != bool(col[5])):
                            modified_fields.append(f)
                removed_fields = [col for col in existing_columns if col not in [f["name"] for f in self.fields]]

                if new_fields:
                    for field in new_fields:
                        col_def = f'"{field["name"]}" {field["type"]}'
                        if field["not_null"] and not field["default"]:
                            col_def += " NOT NULL"
                        if field["default"]:
                            col_def += f" DEFAULT {field['default']}"
                        sql = f'ALTER TABLE "{table_name}" ADD COLUMN {col_def}'
                        self.db_manager.cursor.execute(sql)

                if modified_fields or removed_fields:
                    temp_name = f"{table_name}_temp"
                    create_sql = self.schema_box.toPlainText().replace(f'"{table_name}"', f'"{temp_name}"')
                    self.db_manager.cursor.executescript(create_sql)

                    new_columns = [f'"{f["name"]}"' for f in self.fields]
                    old_columns = [f'"{col}"' for col in existing_columns if col in [f["name"] for f in self.fields]]
                    if not old_columns:
                        insert_sql = f'INSERT INTO "{temp_name}" ({",".join(new_columns)}) VALUES ({",".join(["NULL" for _ in new_columns])})'
                        self.db_manager.cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                        row_count = self.db_manager.cursor.fetchone()[0]
                        for _ in range(row_count):
                            self.db_manager.cursor.execute(insert_sql)
                    else:
                        insert_sql = f'INSERT INTO "{temp_name}" ({",".join(new_columns)}) SELECT {",".join(old_columns + ["NULL" for _ in range(len(new_columns) - len(old_columns))])} FROM "{table_name}"'
                        self.db_manager.cursor.execute(insert_sql)

                    self.db_manager.cursor.execute(f'DROP TABLE "{table_name}"')
                    self.db_manager.cursor.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')

                self.db_manager.conn.commit()
                QMessageBox.information(self, "Success", "Changes applied")
                self.table_changed.emit()  # Notify other tabs
            else:
                sql = self.schema_box.toPlainText()
                self.db_manager.cursor.execute(sql)
                self.db_manager.conn.commit()
                QMessageBox.information(self, "Success", "Table created")
                self.table_changed.emit()  # Notify other tabs
            self.db_manager.metadata_cache.clear()
            self.refresh_tables()
            self.original_fields = self.fields.copy()
        except RuntimeError as e:
            logging.error(f"Failed to apply changes due to lock: {str(e)}")
            QMessageBox.warning(self, "Database Busy", str(e))
        except sqlite3.Error as e:
            logging.error(f"Failed to apply changes: {str(e)}")
            QMessageBox.critical(self, "Error",
                                 f"Failed to apply changes: {str(e)}. Ensure foreign key references are valid.")
            self.db_manager.conn.rollback()
        except Exception as e:
            logging.error(f"Unexpected error during apply_changes: {str(e)}")
            QMessageBox.critical(self, "Error", f"Unexpected error: {str(e)}")
            self.db_manager.conn.rollback()

    def closeEvent(self, event):
        if self.fields != self.original_fields:
            reply = QMessageBox.warning(self, "Unsaved Changes",
                                        "You have unsaved changes. Save them?",
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No |
                                        QMessageBox.StandardButton.Cancel)
            if reply == QMessageBox.StandardButton.Yes:
                self.apply_changes()
                event.accept()
            elif reply == QMessageBox.StandardButton.No:
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()

# ----------------------
# ImportExportTab
# ----------------------
class ImportExportTab(QWidget):
    """Widget for importing and exporting table data."""
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        table_layout = QHBoxLayout()
        table_layout.addWidget(QLabel("Table:"))
        self.table_combo = QComboBox()
        self.table_combo.currentTextChanged.connect(self.refresh_ui)
        table_layout.addWidget(self.table_combo)
        layout.addLayout(table_layout)

        button_layout = QHBoxLayout()
        export_btn = QPushButton("Export to CSV")
        export_btn.clicked.connect(self.export_to_csv)
        button_layout.addWidget(export_btn)
        import_btn = QPushButton("Import CSV")
        import_btn.clicked.connect(self.import_from_csv)
        button_layout.addWidget(import_btn)
        layout.addLayout(button_layout)

        self.refresh_tables()

    def refresh_tables(self):
        self.table_combo.clear()
        self.table_combo.addItem("Select Table")
        if self.db_manager.is_connected():
            self.table_combo.addItems(self.db_manager.get_tables())

    def refresh_ui(self, table_name):
        pass

    def export_to_csv(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table":
            QMessageBox.critical(self, "Error", "Select a table")
            return
        file_path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "", "CSV Files (*.csv);;All Files (*)")
        if file_path:
            try:
                self.db_manager.cursor.execute(f'SELECT * FROM "{table_name}"')
                rows = self.db_manager.cursor.fetchall()
                info = self.db_manager.get_table_info(table_name)
                columns = [col[1] for col in info["columns"]]
                with open(file_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(rows)
                QMessageBox.information(self, "Success", f"Exported {table_name} to {file_path}")
            except Exception as e:
                logging.error(f"Failed to export to CSV: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to export: {str(e)}")

    def import_from_csv(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table":
            QMessageBox.critical(self, "Error", "Select a table")
            return
        file_path, _ = QFileDialog.getOpenFileName(self, "Import CSV", "", "CSV Files (*.csv);;All Files (*)")
        if file_path:
            try:
                info = self.db_manager.get_table_info(table_name)
                columns = [col[1] for col in info["columns"]]
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    if header:
                        header = [h.strip() for h in header]
                        if header != columns[:len(header)]:
                            QMessageBox.critical(self, "Error", "CSV column names do not match table columns")
                            return
                    for row in reader:
                        values = [row[i] if i < len(row) else None for i in range(len(columns))]
                        column_names = ", ".join(f'"{c}"' for c in columns)
                        placeholders = ", ".join(["?" for _ in columns])
                        query = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
                        self.db_manager.cursor.execute(query, values)
                self.db_manager.conn.commit()
                QMessageBox.information(self, "Success", f"Imported data into {table_name}")
                self.db_manager.metadata_cache.clear()
            except RuntimeError as e:
                logging.error(f"Failed to import from CSV due to lock: {str(e)}")
                QMessageBox.warning(self, "Database Busy", str(e))
            except sqlite3.IntegrityError as e:
                logging.error(f"Failed to import from CSV: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to import: {str(e)}. Ensure foreign key values are valid.")
            except Exception as e:
                logging.error(f"Failed to import from CSV: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to import: {str(e)}")
                self.db_manager.conn.rollback()

# ----------------------
# QueryEditor
# ----------------------
class QueryEditor(QWidget):
    """Widget for executing SQL queries with syntax checking."""
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.init_ui()

    def init_ui(self):
        layout = QHBoxLayout(self)
        self.query_input = QTextEdit()
        self.query_input.textChanged.connect(self.check_syntax)
        layout.addWidget(self.query_input, 1)

        self.result_output = QTextEdit()
        self.result_output.setReadOnly(True)
        layout.addWidget(self.result_output, 1)

        execute_btn = QPushButton("Execute Query")
        execute_btn.clicked.connect(self.execute_query)
        layout.addWidget(execute_btn)

    def check_syntax(self):
        query = self.query_input.toPlainText().strip()
        if not query:
            self.query_input.setToolTip("")
            return
        try:
            self.query_input.blockSignals(True)
            if not sqlite3.complete_statement(query):
                fmt = QTextCharFormat()
                fmt.setForeground(QColor("red"))
                cursor = self.query_input.textCursor()
                cursor.select(QTextCursor.SelectionType.Document)
                cursor.setCharFormat(fmt)
                self.query_input.setToolTip("Syntax error: Incomplete SQL statement.")
            else:
                fmt = QTextCharFormat()
                fmt.setForeground(QColor("black"))
                cursor = self.query_input.textCursor()
                cursor.select(QTextCursor.SelectionType.Document)
                cursor.setCharFormat(fmt)
                self.query_input.setToolTip("Valid SQL statement.")
        except Exception as e:
            logging.error(f"Syntax check failed: {str(e)}")
            self.query_input.setToolTip(f"Syntax error: {str(e)}")
        finally:
            self.query_input.blockSignals(False)

    def execute_query(self):
        query = self.query_input.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "Warning", "Enter a query to execute.")
            return
        try:
            if not sqlite3.complete_statement(query):
                QMessageBox.critical(self, "Error", "Invalid SQL syntax. Please correct the query.")
                return
            self.db_manager.cursor.execute(query)
            results = self.db_manager.cursor.fetchall()
            if results:
                output = "\n".join([str(row) for row in results])
                self.result_output.setText(output)
            else:
                self.result_output.setText("Query executed successfully. No results returned.")
            self.db_manager.conn.commit()
        except RuntimeError as e:
            logging.error(f"Query execution failed due to lock: {str(e)}")
            QMessageBox.warning(self, "Database Busy", str(e))
        except sqlite3.Error as e:
            logging.error(f"Query execution failed: {str(e)}")
            QMessageBox.critical(self, "Error", f"Query failed: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error in execute_query: {str(e)}")
            QMessageBox.critical(self, "Error", f"Unexpected error: {str(e)}")

    def refresh_tables(self):
        pass

# ----------------------
# DataBrowser
# ----------------------
class DataBrowser(QWidget):
    """Widget for browsing and editing table data with pagination."""
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.sort_states = {}
        self.filters = {}
        self.current_page = 1
        self.page_size = 1000
        self.total_rows = 0
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        table_layout = QHBoxLayout()
        table_layout.addWidget(QLabel("Table:"))
        self.table_combo = QComboBox()
        self.table_combo.currentTextChanged.connect(self.load_table)
        table_layout.addWidget(self.table_combo)
        layout.addLayout(table_layout)

        self.filter_widget = QWidget()
        self.filter_layout = QHBoxLayout(self.filter_widget)
        layout.addWidget(self.filter_widget)

        self.data_table = QTableWidget()
        self.data_table.setHorizontalHeaderLabels([])
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.data_table.cellChanged.connect(self.cell_changed)
        layout.addWidget(self.data_table)

        controls_layout = QHBoxLayout()
        add_row_btn = QPushButton("Add Row")
        add_row_btn.clicked.connect(self.add_row)
        controls_layout.addWidget(add_row_btn)
        remove_row_btn = QPushButton("Remove Row")
        remove_row_btn.clicked.connect(self.remove_row)
        controls_layout.addWidget(remove_row_btn)
        save_btn = QPushButton("Save Changes")
        save_btn.clicked.connect(self.save_changes)
        controls_layout.addWidget(save_btn)

        preview_btn = QPushButton("Preview")
        preview_btn.setToolTip("Print preview of current page")
        preview_btn.clicked.connect(lambda: self._run_report(preview=True))
        controls_layout.addWidget(preview_btn)

        print_btn = QPushButton("Print")
        print_btn.setToolTip("Send current page to default printer")
        print_btn.clicked.connect(lambda: self._run_report(preview=False))
        controls_layout.addWidget(print_btn)

        page_layout = QHBoxLayout()
        self.page_size_spin = QSpinBox()
        self.page_size_spin.setRange(100, 5000)
        self.page_size_spin.setValue(self.page_size)
        self.page_size_spin.valueChanged.connect(self.set_page_size)
        page_layout.addWidget(QLabel("Page Size:"))
        page_layout.addWidget(self.page_size_spin)
        self.prev_btn = QPushButton("Previous")
        self.prev_btn.clicked.connect(self.prev_page)
        self.prev_btn.setEnabled(False)
        page_layout.addWidget(self.prev_btn)
        self.page_label = QLabel(f"Page 1")
        page_layout.addWidget(self.page_label)
        self.next_btn = QPushButton("Next")
        self.next_btn.clicked.connect(self.next_page)
        self.next_btn.setEnabled(False)
        page_layout.addWidget(self.next_btn)
        controls_layout.addLayout(page_layout)
        layout.addLayout(controls_layout)

        truncate_btn = QPushButton("Truncate All")
        truncate_btn.clicked.connect(self.truncate_all)
        layout.addWidget(truncate_btn, alignment=Qt.AlignmentFlag.AlignRight)

        self.changes = []  # Store (action, row_idx) or (action, rowid, col_idx, value)
        self.refresh_tables()

    def _run_report(self, preview=True):
        if self.table_combo.currentText() == "Select Table":
            QMessageBox.information(self, "Report", "No table selected.")
            return
        if not self.data_table.rowCount() or not self.data_table.columnCount():
            QMessageBox.warning(self, "Warning", "Table data is not loaded. Please select a table and wait for data to load.")
            return
        rep = TableReport(self.data_table, title=f"{self.table_combo.currentText()} – Page {self.current_page}")
        try:
            if preview:
                rep.preview(self)
            else:
                rep.print_(self)
        except Exception as e:
            logging.error(f"Report error: {e}")
            QMessageBox.critical(self, "Print Error", f"Failed to generate report: {str(e)}")

    def refresh_tables(self):
        self.table_combo.clear()
        self.data_table.clear()
        self.data_table.setRowCount(0)
        self.data_table.setColumnCount(0)
        self.changes = []
        self.filters.clear()
        self.sort_states.clear()
        self.current_page = 1
        self.total_rows = 0
        self.page_label.setText(f"Page 1")
        self.prev_btn.setEnabled(False)
        self.next_btn.setEnabled(False)
        for i in reversed(range(self.filter_layout.count())):
            widget = self.filter_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        if self.db_manager.is_connected():
            tables = self.db_manager.get_tables()
            self.table_combo.addItems(tables)
        logging.debug("Tables refreshed")

    def load_table(self, table_name):
        self.data_table.blockSignals(True)
        self.data_table.clear()
        self.data_table.setRowCount(0)
        self.data_table.setColumnCount(0)
        self.changes = []
        self.filters.clear()
        self.sort_states.clear()
        self.current_page = 1
        for i in reversed(range(self.filter_layout.count())):
            widget = self.filter_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        if not table_name or table_name == "Select Table":
            self.total_rows = 0
            self.page_label.setText(f"Page 1")
            self.prev_btn.setEnabled(False)
            self.next_btn.setEnabled(False)
            self.data_table.blockSignals(False)
            logging.debug("No table selected")
            return
        try:
            info = self.db_manager.get_table_info(table_name)
            columns = [col[1] for col in info["columns"]]
            self.data_table.setColumnCount(len(columns))
            self.data_table.setHorizontalHeaderLabels(columns)
            headers = [self.data_table.horizontalHeaderItem(i).text() if self.data_table.horizontalHeaderItem(i) else f"Col{i}" for i in range(len(columns))]
            logging.debug(f"load_table: Set {len(columns)} columns, headers={headers}")
            for col_idx in range(len(columns)):
                filter_edit = QLineEdit()
                filter_edit.setPlaceholderText(columns[col_idx])
                filter_edit.textChanged.connect(lambda text, idx=col_idx: self.apply_filter(idx, text))
                self.filter_layout.addWidget(filter_edit)
            header = self.data_table.horizontalHeader()
            header.setSortIndicatorShown(True)
            header.sortIndicatorChanged.connect(self.sort_table)
            self.update_pagination(table_name)
        except sqlite3.Error as e:
            logging.error(f"Failed to load table in DataBrowser: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to load table: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error in load_table: {str(e)}")
            QMessageBox.critical(self, "Error", f"Unexpected error: {str(e)}")
        finally:
            self.data_table.blockSignals(False)

    def setup_filters_and_update(self, table_name, columns):
        try:
            if self.data_table.columnCount() != len(columns):
                logging.error(f"setup_filters_and_update: Column count mismatch, expected {len(columns)}, got {self.data_table.columnCount()}")
                raise ValueError("Column count mismatch")
            self.filter_layout.setAlignment(Qt.AlignmentFlag.AlignLeft)
            while self.filter_layout.count() < len(columns):
                filter_edit = QLineEdit()
                filter_edit.setPlaceholderText(columns[self.filter_layout.count()])
                filter_edit.textChanged.connect(lambda text, col_idx=self.filter_layout.count(): self.apply_filter(col_idx, text))
                self.filter_layout.addWidget(filter_edit)
            while self.filter_layout.count() > len(columns):
                widget = self.filter_layout.itemAt(self.filter_layout.count() - 1).widget()
                if widget:
                    widget.deleteLater()
            header = self.data_table.horizontalHeader()
            header.setSortIndicatorShown(True)
            header.sortIndicatorChanged.connect(self.sort_table)
            logging.debug(f"setup_filters_and_update: Loaded table {table_name} with {len(columns)} columns")
            self.update_pagination(table_name)
        except Exception as e:
            logging.error(f"Error in setup_filters_and_update: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to set up filters: {str(e)}")

    def update_pagination(self, table_name):
        try:
            query = f"SELECT COUNT(*) FROM \"{table_name}\""
            params = []
            column_count = self.data_table.columnCount()
            logging.debug(f"update_pagination: Updating for {table_name}, column_count={column_count}, filters={self.filters}")
            for col_idx in range(column_count):
                filter_text = self.filters.get(col_idx, "")
                if filter_text:
                    if not params:
                        query += " WHERE"
                    else:
                        query += " AND"
                    header_item = self.data_table.horizontalHeaderItem(col_idx)
                    if header_item and col_idx < column_count:
                        query += f" \"{header_item.text()}\" LIKE ?"
                        params.append(f"%{filter_text}%")
                    else:
                        logging.warning(f"update_pagination: Invalid header item or index {col_idx} for column count {column_count}, skipping filter")
            self.db_manager.cursor.execute(query, params)
            self.total_rows = self.db_manager.cursor.fetchone()[0]
            if self.total_rows == 0:
                self.page_label.setText("Page 0 of 0")
                self.prev_btn.setEnabled(False)
                self.next_btn.setEnabled(False)
                self.data_table.setRowCount(0)
                return
            total_pages = max(1, (self.total_rows + self.page_size - 1) // self.page_size)
            self.current_page = min(max(1, self.current_page), total_pages)
            self.page_label.setText(f"Page {self.current_page} of {total_pages}")
            self.prev_btn.setEnabled(self.current_page > 1)
            self.next_btn.setEnabled(self.current_page < total_pages)
            self.load_page(table_name)
        except Exception as e:
            logging.error(f"Error in update_pagination: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to update pagination: {str(e)}")

    def load_page(self, table_name):
        info = self.db_manager.get_table_info(table_name)
        columns = [col[1] for col in info["columns"]]
        offset = (self.current_page - 1) * self.page_size
        query = f"SELECT rowid, * FROM \"{table_name}\""
        where_clauses = []
        params = []
        for col_idx in range(self.data_table.columnCount()):
            filter_text = self.filters.get(col_idx, "")
            if filter_text:
                where_clauses.append(f"\"{columns[col_idx]}\" LIKE ?")
                params.append(f"%{filter_text}%")
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        sort_col = self.data_table.horizontalHeader().sortIndicatorSection()
        sort_order = "DESC" if self.data_table.horizontalHeader().sortIndicatorOrder() == Qt.SortOrder.DescendingOrder else "ASC"
        if sort_col >= 0 and sort_col < len(columns):
            query += f" ORDER BY \"{columns[sort_col]}\" {sort_order}"
        query += f" LIMIT ? OFFSET ?"
        params.extend([self.page_size, offset])
        self.db_manager.cursor.execute(query, params)
        rows = self.db_manager.cursor.fetchall()
        self.data_table.blockSignals(True)
        self.data_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            rowid = row[0]  # First column is rowid
            for col_idx, value in enumerate(row[1:], 1):  # Skip rowid
                item = QTableWidgetItem(str(value) if value is not None else "")
                if col_idx == 1:
                    item.setData(Qt.ItemDataRole.UserRole, rowid)
                self.data_table.setItem(row_idx, col_idx - 1, item)
        self.data_table.blockSignals(False)
        logging.info(f"Loaded page {self.current_page} for {table_name} with {len(rows)} records")

    def apply_filter(self, col_idx, text):
        if col_idx < self.data_table.columnCount():
            self.filters[col_idx] = text if text else None
            self.current_page = 1
            table_name = self.table_combo.currentText()
            if table_name and table_name != "Select Table":
                self.update_pagination(table_name)
        else:
            logging.warning(f"Invalid filter index {col_idx} for column count {self.data_table.columnCount()}")

    def sort_table(self, logical_index, order):
        self.current_page = 1
        table_name = self.table_combo.currentText()
        if table_name and table_name != "Select Table":
            self.update_pagination(table_name)

    def next_page(self):
        self.current_page += 1
        table_name = self.table_combo.currentText()
        if table_name and table_name != "Select Table":
            self.update_pagination(table_name)

    def prev_page(self):
        self.current_page -= 1
        table_name = self.table_combo.currentText()
        if table_name and table_name != "Select Table":
            self.update_pagination(table_name)

    def set_page_size(self, size):
        self.page_size = size
        self.current_page = 1
        table_name = self.table_combo.currentText()
        if table_name and table_name != "Select Table":
            self.update_pagination(table_name)

    def add_row(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table":
            QMessageBox.critical(self, "Error", "Select a table")
            return
        row_count = self.data_table.rowCount()
        self.data_table.blockSignals(True)
        self.data_table.insertRow(row_count)
        for col_idx in range(self.data_table.columnCount()):
            self.data_table.setItem(row_count, col_idx, QTableWidgetItem(""))
        self.data_table.blockSignals(False)
        self.changes.append(("insert", row_count))
        logging.info(f"Added row at index {row_count} for {table_name}")

    def remove_row(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table":
            QMessageBox.critical(self, "Error", "Select a table")
            return
        selected_rows = self.data_table.selectedIndexes()
        if not selected_rows:
            QMessageBox.warning(self, "Warning", "Select a row to remove")
            return

        try:
            self.data_table.blockSignals(True)
            for row_idx in sorted({i.row() for i in selected_rows}, reverse=True):
                item = self.data_table.item(row_idx, 0)
                rowid = item.data(Qt.ItemDataRole.UserRole) if item else None
                if rowid is not None:
                    self.db_manager.cursor.execute(f'DELETE FROM "{table_name}" WHERE rowid = ?', (rowid,))
                    self.changes.append(("delete", rowid))
                    logging.info(f"Scheduled deletion of rowid {rowid} from {table_name}")
                else:
                    self.data_table.removeRow(row_idx)
                    self.changes.append(("delete", row_idx))
                    logging.info(f"Removed unsaved row at index {row_idx} from {table_name}")
            self.db_manager.conn.commit()
            self.update_pagination(table_name)
        except sqlite3.Error as e:
            logging.error(f"Failed to remove row from {table_name}: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to remove row: {str(e)}")
            self.db_manager.conn.rollback()
        finally:
            self.data_table.blockSignals(False)

    def cell_changed(self, row, col):
        table_name = self.table_combo.currentText()
        if table_name != "Select Table" and row >= 0:
            item = self.data_table.item(row, col)
            value = item.text() if item else ""
            item = self.data_table.item(row, 0)
            rowid = item.data(Qt.ItemDataRole.UserRole) if item else None
            self.changes.append(("update", rowid, col, value))
            logging.info(f"Cell changed at row {row}, col {col} for {table_name}, rowid={rowid}, value={value}")

    def save_changes(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table" or not self.changes:
            return

        progress = QProgressDialog("Saving changes...", "Cancel", 0, len(self.changes), self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setMinimumDuration(0)

        try:
            self.db_manager.conn.execute("BEGIN TRANSACTION")
            info = self.db_manager.get_table_info(table_name)
            columns = [col[1] for col in info["columns"]]
            pk_col = next((col[1] for col in info["columns"] if col[5]), None)
            pk_idx = columns.index(pk_col) if pk_col else -1

            for i, change in enumerate(self.changes):
                if progress.wasCanceled():
                    self.db_manager.conn.rollback()
                    raise RuntimeError("Save operation cancelled")
                progress.setValue(i)

                if change[0] == "insert":
                    row_idx = change[1]
                    values = [self.data_table.item(row_idx, c).text() if self.data_table.item(row_idx, c) else "" for c in range(self.data_table.columnCount())]
                    if pk_idx >= 0 and not values[pk_idx].strip():
                        self.db_manager.cursor.execute(f'SELECT MAX("{pk_col}") FROM "{table_name}"')
                        max_id = self.db_manager.cursor.fetchone()[0]
                        values[pk_idx] = str((max_id or 0) + 1) if max_id is not None else "1"
                    column_names = ", ".join(f'"{c}"' for c in columns)
                    placeholders = ", ".join(["?" for _ in columns])
                    query = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
                    self.db_manager.cursor.execute(query, values)
                    logging.info(f"Inserted row at index {row_idx} with values {values}")

                elif change[0] == "update":
                    _, rowid, col_idx, value = change
                    if rowid:
                        column_name = columns[col_idx]
                        query = f'UPDATE "{table_name}" SET "{column_name}" = ? WHERE rowid = ?'
                        self.db_manager.cursor.execute(query, (value, rowid))
                        logging.info(f"Updated rowid {rowid}, column {column_name} to {value}")

                elif change[0] == "delete":
                    _, rowid = change
                    if isinstance(rowid, int):
                        query = f'DELETE FROM "{table_name}" WHERE rowid = ?'
                        self.db_manager.cursor.execute(query, (rowid,))
                        logging.info(f"Deleted rowid {rowid}")

            self.db_manager.conn.commit()
            self.changes.clear()
            self.update_pagination(table_name)
            progress.setValue(len(self.changes))
            QMessageBox.information(self, "Success", "Changes saved successfully")
        except RuntimeError as e:
            logging.error(f"Save operation cancelled: {str(e)}")
            QMessageBox.warning(self, "Cancelled", str(e))
            self.db_manager.conn.rollback()
        except sqlite3.Error as e:
            logging.error(f"Failed to save changes: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to save changes: {str(e)}")
            self.db_manager.conn.rollback()
        except Exception as e:
            logging.error(f"Unexpected error in save_changes: {str(e)}")
            QMessageBox.critical(self, "Error", f"Unexpected error: {str(e)}")
            self.db_manager.conn.rollback()
        finally:
            progress.close()

    def truncate_all(self):
        table_name = self.table_combo.currentText()
        if table_name == "Select Table":
            QMessageBox.critical(self, "Error", "Select a table")
            return
        reply = QMessageBox.warning(self, "Truncate Table",
                                    f"Are you sure you want to delete all data from {table_name}?",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.cursor.execute(f'DELETE FROM "{table_name}"')
                self.db_manager.conn.commit()
                self.update_pagination(table_name)
                QMessageBox.information(self, "Success", f"All data from {table_name} deleted")
            except sqlite3.Error as e:
                logging.error(f"Failed to truncate table {table_name}: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to truncate table: {str(e)}")
                self.db_manager.conn.rollback()

# ----------------------
# SQLiteEditor
# ----------------------
class SQLiteEditor(QMainWindow):
    """Main application window."""
    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager.get_instance()
        self.setWindowTitle("SQLite Editor")
        self.resize(1200, 800)
        self.init_ui()

    def init_ui(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("File")
        create_action = QAction("Create DB", self)
        create_action.triggered.connect(self.create_db)
        file_menu.addAction(create_action)
        open_action = QAction("Open DB", self)
        open_action.triggered.connect(self.open_db)
        file_menu.addAction(open_action)
        close_action = QAction("Close DB", self)
        close_action.triggered.connect(self.close_db)
        file_menu.addAction(close_action)
        file_menu.addSeparator()
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        self.db_label = QLabel("No DB")
        layout.addWidget(self.db_label)

        self.tab_widget = QTabWidget()
        self.schema_tab = SchemaEditor(self.db_manager)
        self.browser_tab = DataBrowser(self.db_manager)
        self.imp_tab = ImportExportTab(self.db_manager)
        self.query_tab = QueryEditor(self.db_manager)
        self.tab_widget.addTab(self.schema_tab, "Schema Editor")
        self.tab_widget.addTab(self.browser_tab, "Data Browser")
        self.tab_widget.addTab(self.imp_tab, "Import/Export")
        self.tab_widget.addTab(self.query_tab, "Query Editor")
        layout.addWidget(self.tab_widget)

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._deferred_refresh_tabs)
        self._refresh_timer.setSingleShot(True)

        # Connect SchemaEditor signal to refresh DataBrowser and ImportExportTab
        self.schema_tab.table_changed.connect(self.browser_tab.refresh_tables)
        self.schema_tab.table_changed.connect(self.imp_tab.refresh_tables)

    def _ensure_discard_changes(self) -> bool:
        if hasattr(self.browser_tab, "changes") and self.browser_tab.changes:
            if QMessageBox.question(self, "Unsaved", "Discard unsaved changes?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No:
                return False
        return True

    def create_db(self, checked=False) -> bool:
        logging.debug("Starting create_db")
        if not self._ensure_discard_changes():
            logging.debug("create_db cancelled due to unsaved changes")
            return False
        fp, _ = QFileDialog.getSaveFileName(self, "Create DB", "", "SQLite (*.db *.sqlite *.sqlite3)")
        if fp:
            logging.debug(f"Creating database: {fp}")
            if self.db_manager.connect(fp):
                self.db_label.setText(os.path.basename(fp))
                self._refresh_timer.start(100)
                logging.info(f"Database created: {fp}")
                return True
            else:
                logging.error(f"Failed to create database: {fp}")
                return False
        return False

    def open_db(self, checked=False) -> bool:
        logging.debug("Starting open_db")
        if not self._ensure_discard_changes():
            logging.debug("open_db cancelled due to unsaved changes")
            return False
        fp, _ = QFileDialog.getOpenFileName(self, "Open database", "", "SQLite (*.db *.sqlite *.sqlite3)")
        if fp:
            logging.debug(f"Opening database: {fp}")
            prog = QProgressDialog("Opening database...", None, 0, 0, self)
            prog.setWindowModality(Qt.WindowModality.WindowModal)
            prog.show()
            QApplication.processEvents()
            try:
                if self.db_manager.connect(fp):
                    self.db_label.setText(os.path.basename(fp))
                    self._refresh_timer.start(100)
                    logging.info(f"Database opened: {fp}")
                    return True
                else:
                    logging.error(f"Failed to open database: {fp}")
                    return False
            finally:
                prog.close()
        return False

    def close_db(self, checked=False) -> bool:
        logging.debug("Starting close_db")
        if not self._ensure_discard_changes():
            logging.debug("close_db cancelled due to unsaved changes")
            return False

        self._refresh_timer.stop()
        logging.debug("Refresh timer stopped")

        self.db_manager.close()
        self.db_label.setText("No DB")
        logging.info("Database connection closed")

        if self.schema_tab:
            self.schema_tab.refresh_tables()
        if self.browser_tab:
            self.browser_tab.refresh_tables()
        if self.imp_tab:
            self.imp_tab.refresh_tables()
        if self.query_tab:
            self.query_tab.refresh_tables()

        return True

    def _deferred_refresh_tabs(self):
        logging.debug("Starting deferred tab refresh")
        if not self.db_manager.is_connected():
            logging.debug("No database connected, skipping tab refresh")
            return

        prog = QProgressDialog("Refreshing tabs...", None, 0, 4, self)
        prog.setWindowModality(Qt.WindowModality.WindowModal)
        prog.show()
        QApplication.processEvents()
        try:
            prog.setValue(1)
            logging.debug("Refreshing Schema tab")
            if self.schema_tab:
                self.schema_tab.refresh_tables()
            prog.setValue(2)
            logging.debug("Refreshing DataBrowser tab")
            if self.browser_tab:
                self.browser_tab.refresh_tables()
            prog.setValue(3)
            logging.debug("Refreshing ImportExport tab")
            if self.imp_tab:
                self.imp_tab.refresh_tables()
            prog.setValue(4)
            logging.debug("Tab refresh completed")
        except Exception as e:
            logging.error(f"Error during tab refresh: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to refresh tabs: {str(e)}")
        finally:
            prog.close()

    def closeEvent(self, ev):
        logging.debug("Handling closeEvent")
        if self._ensure_discard_changes():
            self._refresh_timer.stop()
            self.db_manager.close()
            logging.info("Application closed cleanly")
            ev.accept()
        else:
            ev.ignore()
            logging.debug("Close event ignored due to unsaved changes")

def log_method_entry(func):
    def wrapper(*args, **kwargs):
        logging.debug(f"Entering {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SQLiteEditor()
    window.show()
    sys.exit(app.exec())