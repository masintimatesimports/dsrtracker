import streamlit as st
import pandas as pd
from datetime import timedelta
from rapidfuzz import process, fuzz
import re
from datetime import datetime
import plotly.express as px
import requests
from io import BytesIO
import pandas as pd
import streamlit as st
from auth import AuthManager
import streamlit_authenticator as stauth
import requests
from io import BytesIO
import time
import difflib
from functools import lru_cache
import pandas as pd
import requests
from io import BytesIO
import tempfile



# ========================
# CONFIGURATION MANAGER
# ========================
import json
from pathlib import Path
import streamlit as st
import pandas as pd

auth = AuthManager()

class ConfigurationManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigurationManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        self.config_path = Path("config.json")
        self.default_config = {
            "global": {
                "date_range_weeks": 2,
                "fuzzy_threshold": 45,
                "default_bond_type": "FCL"
            },
            "data_sources": {
            "china": {
                "bookings_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRURi444OvCBzhhE8JpoT7B3H-M7Jdx-TYX0yPJ4DbfWcgIYZ1BPU13xB9vfujUow/pub?output=xlsx",
                "delivery_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-GWXG4Zj1ewZlQLDJW6FK6AaJqeD-wE8uS9Cm5OeTQTVx4lICLo11M--sqpAzhqnbs5p2U8Z7V9Os/pub?output=xlsx",
                "sheets": {
                    "bookings": {
                        "default_sheet": "MAY 2025",
                        "tracker_sheet": "AIR"
                    },
                    "delivery": {
                        "air_sheet": "AIR",
                        "sea_sheet": "SEA"
                    }
                }
            },        "sea": {
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-GWXG4Zj1ewZlQLDJW6FK6AaJqeD-wE8uS9Cm5OeTQTVx4lICLo11M--sqpAzhqnbs5p2U8Z7V9Os/pub?output=xlsx",
            "sheet_name": "SEA_Merchant"
        },
        "air": {
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-GWXG4Zj1ewZlQLDJW6FK6AaJqeD-wE8uS9Cm5OeTQTVx4lICLo11M--sqpAzhqnbs5p2U8Z7V9Os/pub?output=xlsx",
            "sheet_name": "AIR"
        },
            "india": {
                "bookings_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQh9PCBs5Z-aq1QV_gBuXYlkv2bjm0-12An8yqVpzntQNctS-vdis7oVnaJ-2BA8J3sCp8U9vYMd2Nh/pub?output=xlsx",
                "default_sheet": "APRIL 25-26",
                "tracker_sheet": "AIR"
            }
            }       
            ,
            "columns": [
                "HBL", "ETA", "SBU", "CARGO READY DATE", "Origin", "Port", 
                "Shipper", "Inv #", "PO #", "Description of Goods", "No of Cartons",
                "Type", "Gross Weight", "Actual CBM", "LCL,FCL Status",
                "Origin Vessel", "Connecting vessel", "Voyage No", "ATD",
                "ATA", "ETB", "Estimated Clearance", "Status", "HB/L NO",
                "Container No.", "Bond or Non Bond", "Cleared By", "REMARK",
                "Delivery date", "Delivery location","CUSDEC No","CUSDEC Date", "sheet"
            ],
            "mappings":{
                    "expo": {
                        "bond": {
                                "HBL": "HBL", "PO #": "PO # ", "Gross Weight": "Gross Weight",
                                "Status": "Status", "Voyage No": 29, "ATD": "ATD",
                                "Description of Goods": "Description of Goods", "ETA": "ETA",
                                "No of Cartons": "No of Cartons", "ATA": "ATA",
                                "CARGO READY DATE": "Shipment Ready Date , Invoice Date",
                                "Origin": "Port of Origin", "Port": "Port of Origin",
                                "Inv #": "SUPPLIER Invoice", "Actual CBM": "CBM (Last two decimals)",
                                "LCL,FCL Status": "LCL,FCL", "Origin Vessel": 27,
                                "Connecting vessel": 28,
                                "Bond or Non Bond": "BOND/Non BOND", "REMARK": "Comments",
                                "Delivery date": "Delivery Date", "Delivery location": "Location",
                                "Container No.": "Container No", "Shipper": "Supplier( Name as for the Invoice)",
                                "Type": " Type (In two letters)", "SBU": "Consignee","CUSDEC No":48,"CUSDEC Date":"Cusdec Date"
                            },
                        "non_bond": {
                            "HBL": "HBL",
                            "PO #": 3,
                            "Gross Weight": "Gross Weight",
                            "Status": "Status",
                            "Voyage No": "Voyage No",
                            "ATD": "LATEST ATD",
                            "Description of Goods": "Description of Goods",
                            "ETA": "ETA",
                            "No of Cartons": "No of Cartons",
                            "ATA": "LATEST ATA",
                            "CARGO READY DATE": "Shipment Ready Date , Invoice Date",
                            "Origin": "Port of Origin",
                            "Port": "Port of Origin",
                            "Inv #": "SUPPLIER Invoice",
                            "Actual CBM": 18,
                            "LCL,FCL Status": "LCL , FCL",
                            "Origin Vessel": 25,
                            "Connecting vessel": "Second Vessel , Flight",
                            "Bond or Non Bond": "BOND/Non BOND",
                            "REMARK": "Comments",
                            "Delivery date": 51,
                            "Delivery location": 53,
                            "Container No.": "Container No",
                            "Shipper": "Supplier( Name as for the Invoice)",
                            "Type": "TYPE (IN TWO LETTERS)",
                            "SBU": "Consignee",
                            "CUSDEC No":50,
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
                        },
                        "fcl": {
                                "HBL": "HBL", "PO #": 3, "Gross Weight": "Gross Weight",
                                "Status": "Status", "Voyage No": "Voyage No", "ATD": "LATEST ATD",
                                "Description of Goods": "Description of Goods", "ETA": "ETA",
                                "No of Cartons": "No of Cartons", "ATA": "LATEST ATA",
                                "CARGO READY DATE": "Shipment Ready Date , Invoice Date",
                                "Origin": "Port of Origin", "Port": "Port of Origin",
                                "Inv #": "SUPPLIER Invoice", "Actual CBM": 18,
                                "LCL,FCL Status": "LCL , FCL", "Origin Vessel": 23,
                                "Connecting vessel": "Second Vessel , Flight",
                                "Bond or Non Bond": "BOND/Non BOND", "REMARK": "Comments",
                                "Delivery date": 52, "Delivery location": 54,
                                "Container No.": "Container No", "Shipper": "Supplier( Name as for the Invoice)",
                                "Type": 14, "SBU": "Consignee",
                            "CUSDEC No":"Cusdec No.",
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
                            }
                    },
                    "maersk": {
                        "dsr": {
        # Direct column name mappings
        "ATA": "ATA",
        "ATD": "ATD",
        "Description of Goods": "Description of Goods",
        "ETA": "ETA",
        "Gross Weight": "Gross Weight",
        "HBL": "HBL",
        "No of Cartons": "No of Cartons",
        "Status": "Status",
        "Voyage No": "Voyage No",
        "Actual CBM": "CBM (Last two decimals)",
        "Container No.": "Container No",
        "LCL,FCL Status": "LCL , FCL",
        "Origin": "Port of Origin",
        "Port": "Port of Origin",
        "Shipper": "Supplier( Name as for the Invoice)",
        "REMARK": "PENDING TASKS",
        "Delivery date": "Delivery Date",
        "Delivery location": "Delivery Location",
        "Inv #": 3,
        "PO #": 4,
        "CARGO READY DATE": 10,
        "Origin Vessel": 24,
        "Connecting vessel": 25,
        "Type": 15,
        "SBU": "Consignee",
                            "CUSDEC No":"Cusdec No.",
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
    },
                        "archived": {
        # Direct column name mappings
        "ATA": "ATA",
        "ATD": "ATD",
        "Description of Goods": "Description of Goods",
        "ETA": "ETA",
        "Gross Weight": "Gross Weight",
        "HBL": "HBL",
        "No of Cartons": "No of Cartons",
        "Status": "Status",
        "Voyage No": "Voyage No",
        "Actual CBM": "CBM (Last two decimals)",
        "Container No.": "Container No",
        "LCL,FCL Status": "LCL , FCL",
        "Origin": "Port of Origin",
        "Port": "Port of Origin",
        "Shipper": "Supplier( Name as for the Invoice)",
        "REMARK": "PENDING TASKS",
        "Delivery date": "Delivery Date",
        "Delivery location": "Delivery Location",
        "Inv #": 3,
        "PO #": 4,
        "CARGO READY DATE": 10,
        "Origin Vessel": 24,
        "Connecting vessel": 25,
        "Type": 15,
        "SBU": "Consignee",
                            "CUSDEC No":"Cusdec No.",
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
    }
                    },
                    "globe": {
                        "ongoing": {
                                    # Exact matches (auto-aligned columns)
                                    "ETA": "ETA DATE",
                                    "PO #": "PO #",
                                    # Manual mappings for missing columns
                                    "ATA":  None,  # Assuming MAS REF # represents ATA (adjust if needed)
                                    "ATD":  None,  # Assuming VESSEL/VOYAGE can correspond to ATD (adjust if needed)
                                    "Actual CBM": "CBM",
                                    "Bond or Non Bond": "FCL",  # Assuming default bond type as FCL
                                    "CARGO READY DATE": "CLEARED DATE",  # Assuming "CLEARED DATE" corresponds to CARGO READY DATE
                                    "Cleared By": None,  # If not available, fill with NaN
                                    "Connecting vessel": None,  # Assuming VESSEL/VOYAGE corresponds to Connecting vessel
                                    "Container No.": "CONTAINER #",
                                    "Delivery date":  None,  # Using ETA as proxy for Delivery Date
                                    "Delivery location": None,  # Assuming Delivery Location is missing
                                    "Description of Goods": "CARGO DESCRIPTION",
                                    "ETB": None,  # Assuming ETB is missing
                                    "Estimated Clearance": None,  # Assuming this is missing
                                    "Gross Weight": "G/W (KG)",
                                    "HB/L NO": "BL #",  # Assuming BL # corresponds to HB/L NO
                                    "HBL": "BL #",  # Assuming BL # corresponds to HBL
                                    "Inv #": None,  # Assuming Inv # is missing
                                    "LCL,FCL Status": None,  # Assuming this is missing
                                    "No of Cartons": None,  # Assuming this is missing
                                    "Origin": None,  # Assuming Origin is missing
                                    "Origin Vessel": "VESSEL/VOYAGE",  # Assuming Origin Vessel is missing
                                    "Port": None,  # Assuming Port is missing
                                    "REMARK": "REMARKS",
                                    "SBU": 2,  # Assuming SBU is missing
                                    "Shipper": "SHIPPER",
                                    "Status": "STATUS",
                                    "Type": None,  # Assuming Type is missing
                                    "Voyage No": None,
                            "CUSDEC No":"ENTRY # / DATE",
                                },
                        "cleared":  {
                                        # Exact matches (auto-aligned columns)
                                        "ETA": "ETA DATE",
                                        "PO #": "PO #",
                                        
                                        # Manual mappings for missing columns
                                        "ATA":  None,  # Assuming MAS REF # represents ATA (adjust if needed)
                                        "ATD":  None,  # Assuming VESSEL/VOYAGE can correspond to ATD (adjust if needed)
                                        "Actual CBM": "CBM",
                                        "Bond or Non Bond": "FCL",  # Assuming default bond type as FCL
                                        "CARGO READY DATE": "CLEARED DATE",  # Assuming "CLEARED DATE" corresponds to CARGO READY DATE
                                        "Cleared By": None,  # If not available, fill with NaN
                                        "Connecting vessel": None,  # Assuming VESSEL/VOYAGE corresponds to Connecting vessel
                                        "Container No.": "CONTAINER #",
                                        "Delivery date":  None,  # Using ETA as proxy for Delivery Date
                                        "Delivery location": None,  # Assuming Delivery Location is missing
                                        "Description of Goods": "CARGO DESCRIPTION",
                                        "ETB": None,  # Assuming ETB is missing
                                        "Estimated Clearance": None,  # Assuming this is missing
                                        "Gross Weight": "G/W (KG)",
                                        "HB/L NO": "BL NO",  # Assuming BL # corresponds to HB/L NO
                                        "HBL": "BL NO",  # Assuming BL # corresponds to HBL
                                        "Inv #": None,  # Assuming Inv # is missing
                                        "LCL,FCL Status": None,  # Assuming this is missing
                                        "No of Cartons": None,  # Assuming this is missing
                                        "Origin": None,  # Assuming Origin is missing
                                        "Origin Vessel": "VESSEL/VOYAGE",  # Assuming Origin Vessel is missing
                                        "Port": None,  # Assuming Port is missing
                                        "REMARK": "REMARKS",
                                        "SBU": 2,  # Assuming SBU is missing
                                        "Shipper": "SHIPPER",
                                        "Status": "STATUS",
                                        "Type": None,  # Assuming Type is missing
                                        "Voyage No": None,
                                        "CUSDEC No":"ENTRY #",
  # Assuming Voyage No is missing
                                    }

                    },
                    "scanwell": {
                        "unichela": {
                                "ETA": "ETA", "ATD": "ATD", "Bond or Non Bond": "Bond or Non Bond",
                                "Gross Weight": "Gross Weight", "No of Cartons": "No of Cartons", 
                                "Voyage No": "Voyage No", "ATA": " ATA", "Actual CBM": "CBM",
                                "CARGO READY DATE": "Shipment Ready Date/Invoice Date", "Cleared By": None,
                                "Connecting vessel": "Second Vessel", "Container No.": "Container No",
                                "Delivery date": "Delivery Date", "Delivery location": None,
                                "Description of Goods": "Discriptin of Goods", "ETB": None,
                                "Estimated Clearance": "Planned Clearance", "HB/L NO": "HBL NO",
                                "HBL": "HBL NO", "Inv #": "IA1:AC1NVOICE", "LCL,FCL Status": "LCL/FCL",
                                "Origin": "Port of Origin", "Origin Vessel": "First Vessel", "PO #": 1,
                                "Port": "Port of Origin", "REMARK": "Comments", "SBU": 3,
                                "Shipper": "Supplier", "Status": "Pre Alert Status", "Type": "CTN Type",
                                "CUSDEC No":"Cusdec No",
                            },
                        "bodyline": {
                            "ETA": "ETA", "ATD": "ATD", "Bond or Non Bond": "Bond or Non Bond",
                            "Gross Weight": "Gross Weight", "No of Cartons": "No of Cartons", 
                            "Voyage No": "Voyage No", "ATA": " ATA", "Actual CBM": "CBM",
                            "CARGO READY DATE": "Shipment Ready Date/Invoice Date", "Cleared By": None,
                            "Connecting vessel": "Second Vessel", "Container No.": "Container No",
                            "Delivery date": "Delivery Date", "Delivery location": None,
                            "Description of Goods": "Discriptin of Goods", "ETB": None,
                            "Estimated Clearance": "Planned Clearance", "HB/L NO": "HBL NO",
                            "HBL": "HBL NO", "Inv #": "INVOICE", "LCL,FCL Status": "LCL/FCL",
                            "Origin": "Port of Origin", "Origin Vessel": "First Vessel", "PO #": 1,
                            "Port": "Port of Origin", "REMARK": "Remarks", "SBU": 3,
                            "Shipper": "Supplier", "Status": "Pre Alert Status", "Type": "CTN Type",
                            "CUSDEC No":"Cusdec No",

                        }
                    }
                }
                ,
            "target_consignees": ["Unichela", "MAS Capital", "Bodyline"]
        }
        self.config = self._load_config()
        
    def _load_config(self):
        try:
            if self.config_path.exists():
                with open(self.config_path, "r") as f:
                    return json.load(f)
            return self.default_config
        except Exception as e:
            st.error(f"Error loading config: {str(e)}")
            return self.default_config
        
    def save_config(self):
        try:
            # Convert string numbers back to integers before saving
            def convert_numbers(obj):
                if isinstance(obj, dict):
                    return {k: convert_numbers(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numbers(item) for item in obj]
                elif isinstance(obj, str) and obj.isdigit():
                    return int(obj)
                return obj
                
            with open(self.config_path, "w") as f:
                json.dump(convert_numbers(self.config), f, indent=4)
            return True
        except Exception as e:
            st.error(f"Error saving config: {str(e)}")
            return False

            
    def get_all_columns(self):
        """Combine required and optional columns"""
        return self.config["columns"]
        # return self.config["columns"]["required"] + self.config["columns"]["optional"]
        
    def get_mappings(self, file_type, sheet_type):
        """Get mappings for specific file and sheet type"""
        try:
            return self.config["mappings"][file_type][sheet_type]
        except KeyError:
            return {}
            
    def update_mappings(self, file_type, sheet_type, new_mappings):
        """Update mappings for a specific sheet"""
        if file_type not in self.config["mappings"]:
            self.config["mappings"][file_type] = {}
        self.config["mappings"][file_type][sheet_type] = new_mappings
        
    def add_column(self, column_name, is_required=False):
        """Add a new column to configuration"""
        target_list = "required" if is_required else "optional"
        if column_name not in self.config["columns"][target_list]:
            self.config["columns"][target_list].append(column_name)
            return True
        return False
        
    def remove_column(self, column_name):
        """Remove a column from configuration"""
        # Don't allow removal of required columns
        if column_name in self.config["columns"]["required"]:
            return False
            
        if column_name in self.config["columns"]["optional"]:
            self.config["columns"]["optional"].remove(column_name)
            
            # Clean up any mappings using this column
            for file_type in self.config["mappings"]:
                for sheet_type in self.config["mappings"][file_type]:
                    if column_name in self.config["mappings"][file_type][sheet_type]:
                        del self.config["mappings"][file_type][sheet_type][column_name]
            return True
        return False

    def validate_config(self):
        errors = []
        # Check mappings reference valid columns
        for file_type, sheets in self.config.get("mappings", {}).items():
            for sheet_type, mappings in sheets.items():
                for target_col in mappings.keys():
                    if target_col not in self.get_all_columns():
                        errors.append(f"Mapping references non-existent column: {target_col} in {file_type}/{sheet_type}")
        return errors



# Initialize configuration manager
config_manager = ConfigurationManager()

# ========================
# CONFIGURATION UI
# ========================
def show_configuration_ui():
    st.sidebar.header("Configuration")
    
    with st.sidebar.expander("⚙️ Settings", expanded=False):
        tab1, tab2, tab3, tab4 = st.tabs(["Columns", "Mappings", "Global", "Data Sources"])
        
        with tab1:
            show_column_management()
            
        with tab2:
            show_mapping_management()
            
        with tab3:
            show_global_settings()

        with tab4:
            show_data_sources_management()

def show_column_management():
    st.subheader("Column Management")
    
    # Single list of all columns
    st.write("**All Columns**")
    columns_df = st.data_editor(
        pd.DataFrame(config_manager.config["columns"], columns=["Column"]),
        num_rows="dynamic",
        key="columns_editor",
        hide_index=True
    )
    
    # Add new column
    st.subheader("Add New Column")
    new_col = st.text_input("Column Name", key="new_column_name")
    
    if st.button("Add Column", key="add_column_btn"):
        if not new_col.strip():
            st.error("Column name cannot be empty")
        elif new_col.strip() not in config_manager.config["columns"]:
            config_manager.config["columns"].append(new_col.strip())
            if config_manager.save_config():
                st.success(f"Added column: {new_col}")
                st.rerun()
        else:
            st.error(f"Column '{new_col}' already exists")
    
    # Remove column
    st.subheader("Remove Column")
    col_to_remove = st.selectbox(
        "Select column to remove",
        [""] + config_manager.config["columns"],
        key="col_to_remove"
    )
    
    if col_to_remove and st.button("Remove Column", key="remove_column_btn"):
        if col_to_remove in config_manager.config["columns"]:
            config_manager.config["columns"].remove(col_to_remove)
            # Clean up any mappings using this column
            for file_type in config_manager.config["mappings"]:
                for sheet_type in config_manager.config["mappings"][file_type]:
                    if col_to_remove in config_manager.config["mappings"][file_type][sheet_type]:
                        del config_manager.config["mappings"][file_type][sheet_type][col_to_remove]
            if config_manager.save_config():
                st.success(f"Removed column: {col_to_remove}")
                st.rerun()


def show_mapping_management():
    st.subheader("Mapping Management")
    
    file_type = st.selectbox(
        "Select File Type",
        ["expo", "maersk", "globe", "scanwell"],
        key="mapping_file_type"
    )
    
    sheet_type = st.selectbox(
        "Select Sheet Type",
        list(config_manager.config["mappings"].get(file_type, {}).keys()),
        key="mapping_sheet_type"
    )
    
    if not sheet_type:
        st.warning(f"No sheets defined for {file_type}")
        return
    
    current_mappings = config_manager.get_mappings(file_type, sheet_type)
    updated_mappings = {}
    
    for target_col in config_manager.get_all_columns():
        current_val = current_mappings.get(target_col, "")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            st.markdown(f"**{target_col}**")
        
        with col2:
            # Use text input but validate for numbers
            input_val = st.text_input(
                "Source column or index",
                value=str(current_val) if current_val is not None else "",
                key=f"mapping_{file_type}_{sheet_type}_{target_col}",
                label_visibility="collapsed"
            )
            
            # Convert to int if it's a number, otherwise keep as string
            updated_mappings[target_col] = int(input_val) if input_val.isdigit() else input_val
    
    if st.button("Save Mappings", key="save_mappings_btn"):
        # Filter out empty mappings but keep 0 as valid index
        config_manager.update_mappings(
            file_type,
            sheet_type,
            {k: v for k, v in updated_mappings.items() if v != ""}
        )
        if config_manager.save_config():
            st.success("Mappings saved!")
        else:
            st.error("Failed to save mappings")

def show_global_settings():
    st.subheader("Global Settings")
    
    # Date range weeks
    weeks = st.number_input(
        "Date Range Weeks",
        min_value=1,
        max_value=8,
        value=config_manager.config["global"]["date_range_weeks"],
        key="date_range_weeks_cfg"
    )
    
    # Fuzzy threshold
    threshold = st.slider(
        "Fuzzy Matching Threshold",
        min_value=0,
        max_value=100,
        value=config_manager.config["global"]["fuzzy_threshold"],
        key="fuzzy_threshold_cfg"
    )
    
    # Default bond type
    bond_type = st.selectbox(
        "Default Bond Type",
        ["FCL", "LCL", "Non Bond"],
        index=["FCL", "LCL", "Non Bond"].index(config_manager.config["global"]["default_bond_type"]),
        key="default_bond_type_cfg"
    )
    
    # Target consignees
    st.subheader("Target Consignees")
    consignees = st.text_area(
        "Consignees (one per line)",
        value="\n".join(config_manager.config["target_consignees"]),
        key="target_consignees_cfg"
    )
    
    if st.button("Save Global Settings", key="save_global_btn"):
        config_manager.config["global"].update({
            "date_range_weeks": weeks,
            "fuzzy_threshold": threshold,
            "default_bond_type": bond_type
        })
        config_manager.config["target_consignees"] = [
            c.strip() for c in consignees.split("\n") if c.strip()
        ]
        
        errors = config_manager.validate_config()
        if errors:
            for error in errors:
                st.error(error)
        elif config_manager.save_config():
            st.success("Global settings saved!")
        else:
            st.error("Failed to save global settings")

def show_data_sources_management():
    st.subheader("Data Source Configuration")
    
    region = st.selectbox(
        "Select Region",
        ["china", "india"],
        key="data_source_region"
    )
    
    st.markdown("---")
    st.subheader(f"{region.upper()} Configuration")
    
    # Get current config for the region
    region_config = config_manager.config["data_sources"].get(region, {})
    
    # URLs
    col1, col2 = st.columns(2)
    with col1:
        bookings_url = st.text_input(
            "Bookings URL",
            value=region_config.get("bookings_url", ""),
            key=f"{region}_bookings_url"
        )
    
    if region == "china":
        with col2:
            delivery_url = st.text_input(
                "Delivery URL",
                value=region_config.get("delivery_url", ""),
                key=f"{region}_delivery_url"
            )
    
    # Sheet names
    st.subheader("Sheet Names")
    
    if region == "china":
        # Initialize sheets config if not exists
        if "sheets" not in region_config:
            region_config["sheets"] = {
                "bookings": {},
                "delivery": {}
            }
        
        col1, col2 = st.columns(2)
        with col1:
            bookings_sheet = st.text_input(
                "Default Bookings Sheet",
                value=region_config["sheets"]["bookings"].get("default_sheet", ""),
                key=f"{region}_bookings_sheet"
            )
            
            tracker_sheet = st.text_input(
                "Tracker Sheet (Bookings)",
                value=region_config["sheets"]["bookings"].get("tracker_sheet", ""),
                key=f"{region}_tracker_sheet"
            )
        
        with col2:
            air_sheet = st.text_input(
                "AIR Delivery Sheet",
                value=region_config["sheets"]["delivery"].get("air_sheet", ""),
                key=f"{region}_air_sheet"
            )
            
            sea_sheet = st.text_input(
                "SEA Delivery Sheet",
                value=region_config["sheets"]["delivery"].get("sea_sheet", ""),
                key=f"{region}_sea_sheet"
            )
    else:  # india
        default_sheet = st.text_input(
            "Default Sheet",
            value=region_config.get("default_sheet", ""),
            key=f"{region}_default_sheet"
        )
        
        tracker_sheet = st.text_input(
            "Tracker Sheet",
            value=region_config.get("tracker_sheet", ""),
            key=f"{region}_tracker_sheet"
        )
    
    if st.button("Save Data Source Configuration", key=f"save_{region}_data_source"):
        # Update the config structure
        if region not in config_manager.config["data_sources"]:
            config_manager.config["data_sources"][region] = {}
        
        # Update URLs
        config_manager.config["data_sources"][region]["bookings_url"] = bookings_url
        if region == "china":
            config_manager.config["data_sources"][region]["delivery_url"] = delivery_url
        
        # Update sheet names
        if region == "china":
            if "sheets" not in config_manager.config["data_sources"][region]:
                config_manager.config["data_sources"][region]["sheets"] = {
                    "bookings": {},
                    "delivery": {}
                }
            
            config_manager.config["data_sources"][region]["sheets"]["bookings"]["default_sheet"] = bookings_sheet
            config_manager.config["data_sources"][region]["sheets"]["bookings"]["tracker_sheet"] = tracker_sheet
            config_manager.config["data_sources"][region]["sheets"]["delivery"]["air_sheet"] = air_sheet
            config_manager.config["data_sources"][region]["sheets"]["delivery"]["sea_sheet"] = sea_sheet
        else:
            config_manager.config["data_sources"][region]["default_sheet"] = default_sheet
            config_manager.config["data_sources"][region]["tracker_sheet"] = tracker_sheet
        
        if config_manager.save_config():
            st.success("Data source configuration saved!")
            st.rerun()
        else:
            st.error("Failed to save configuration")


def init_session_state():
    session_defaults = {
        'current_step': 1,  # 1=Expo, 2=Maersk, 3=Type3, 4=Type4, 5=Final
        'expo_data': pd.DataFrame(),
        'maersk_data': pd.DataFrame(),  # New Maersk data storage
        'globe_data': pd.DataFrame(),
        'scanwell_data': pd.DataFrame(),
        'scanwell_processed': False,        'expo_processed': False,
        'maersk_processed': False,  # New Maersk status flag
        'globe_processed': False,
        'current_file': None,
        'selected_reference_date': pd.Timestamp.today().normalize(),  # NEW
        'config_manager': config_manager,  # Add this line
        # Optional: Track individual Maersk sheets if needed
        'maersk_dsr_processed': False,
        'maersk_archived_processed': False,
        'scanwell_unichela_processed': False,
        'scanwell_bodyline_processed': False
    }
    
    for key, value in session_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()


# ========================
# COMMON PROCESSING LOGIC
# ========================


# Initialize ConfigurationManager at the top (after imports)
config_manager = ConfigurationManager()

# Create helper functions to access configuration
def get_target_columns():
    """Get combined list of required and optional columns"""
    return config_manager.get_all_columns()

def get_target_consignees():
    """Get current target consignees"""
    return config_manager.config["target_consignees"]

def get_fuzzy_threshold():
    """Get current fuzzy matching threshold"""
    return config_manager.config["global"]["fuzzy_threshold"]

def get_date_range_weeks():
    """Get current date range setting"""
    return config_manager.config["global"]["date_range_weeks"]

# def filter_and_match_consignee(df, target_consignees=None, date_column="ETA", threshold=None):
    try:
        # Use configured values if not provided
        if target_consignees is None:
            target_consignees = get_target_consignees()
        if threshold is None:
            threshold = get_fuzzy_threshold()
            
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        # Use configured date range
        ref_date = st.session_state.selected_reference_date
        weeks = get_date_range_weeks()
        start_date = ref_date - timedelta(weeks=weeks)
        end_date = ref_date + timedelta(weeks=weeks)

        # Rest of the function remains the same...
        # ... [keep all existing code]
        # Filter rows based on ETA date range
        filtered_by_eta = df[
            (df[date_column] >= start_date) & (df[date_column] <= end_date)
        ].copy()

        # Check if there are any rows after filtering
        if filtered_by_eta.empty:
            return pd.DataFrame(), pd.DataFrame()  # Return empty DataFrames if no rows match the date filter

        # Normalize the consignee names for matching
        def normalize_text(text):
            return ''.join(e for e in str(text).lower() if e.isalnum()) if pd.notna(text) else ""

        filtered_by_eta["Consignee_clean"] = filtered_by_eta["Consignee"].apply(normalize_text)
        normalized_targets = [normalize_text(name) for name in target_consignees]

        # Function to get the best match using fuzzy matching
        def get_best_match_score(text):
            if not text:
                return pd.Series([pd.NA, 0])
            match_data = process.extractOne(text, normalized_targets, scorer=fuzz.token_set_ratio)
            if not match_data:
                return pd.Series([pd.NA, 0])
            return pd.Series([match_data[0], match_data[1]])

        filtered_by_eta[["BestMatch", "Score"]] = filtered_by_eta["Consignee_clean"].apply(get_best_match_score)

        # Filter rows based on matching score
        matched_df = filtered_by_eta[filtered_by_eta["Score"] >= threshold].copy()
        removed_df = filtered_by_eta[filtered_by_eta["Score"] < threshold].copy()

        # Check if matched_df is empty and handle accordingly
        if matched_df.empty:
            return pd.DataFrame(), removed_df  # Return empty matched_df and removed_df with the non-matching rows

        return matched_df, removed_df

    except Exception as e:
        print(f"❌ Error in filter_and_match_consignee: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def filter_and_match_consignee(df, target_consignees=None, date_column="ETA", threshold=None):
    try:
        # Use configured values if not provided
        if target_consignees is None:
            target_consignees = get_target_consignees()
        if threshold is None:
            threshold = get_fuzzy_threshold()
            
        # Only process date if filtering is enabled and column exists
        if st.session_state.get('use_date_filter', True) and date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            
            # Use configured date range
            ref_date = st.session_state.selected_reference_date
            weeks = get_date_range_weeks()
            start_date = ref_date - timedelta(weeks=weeks)
            end_date = ref_date + timedelta(weeks=weeks)

            # Filter rows based on ETA date range
            filtered_df = df[
                (df[date_column] >= start_date) & (df[date_column] <= end_date)
            ].copy()
        else:
            filtered_df = df.copy()

        # Rest of the function remains the same...
        # Normalize the consignee names for matching
        def normalize_text(text):
            return ''.join(e for e in str(text).lower() if e.isalnum()) if pd.notna(text) else ""

        filtered_df["Consignee_clean"] = filtered_df["Consignee"].apply(normalize_text)
        normalized_targets = [normalize_text(name) for name in target_consignees]

        # Function to get the best match using fuzzy matching
        def get_best_match_score(text):
            if not text:
                return pd.Series([pd.NA, 0])
            match_data = process.extractOne(text, normalized_targets, scorer=fuzz.token_set_ratio)
            if not match_data:
                return pd.Series([pd.NA, 0])
            return pd.Series([match_data[0], match_data[1]])

        filtered_df[["BestMatch", "Score"]] = filtered_df["Consignee_clean"].apply(get_best_match_score)

        # Filter rows based on matching score
        matched_df = filtered_df[filtered_df["Score"] >= threshold].copy()
        removed_df = filtered_df[filtered_df["Score"] < threshold].copy()

        return matched_df, removed_df

    except Exception as e:
        print(f"❌ Error in filter_and_match_consignee: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def process_consignee_matching(df, target_consignees=None, date_column="ETA", consignee_column="Consignee", threshold=None):
    """Processes consignee name matching using fuzzy matching"""
    # Use configured values if not provided
    if target_consignees is None:
        target_consignees = get_target_consignees()
    if threshold is None:
        threshold = get_fuzzy_threshold()
        
    # Rest of the function remains the same...
    # ... [keep all existing code]

    # Step 1: Convert ETA to datetime
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    # Step 2: Add ETA Remark for missing values
    df["ETA Remark"] = df[date_column].apply(lambda x: "NA" if pd.isna(x) else "")

    # Step 3: Rename to standard column names for processing
    df = df.rename(columns={consignee_column: "Consignee", date_column: "ETA"})

    # Step 4: Apply filtering and fuzzy matching
    matched_df, removed_df = filter_and_match_consignee(df, target_consignees, date_column="ETA", threshold=threshold)


    return matched_df, removed_df

def map_and_append_maersk_data(source_df, final_df, column_mapping, sheet_name="maersk_dsr", default_bond_type=None):
    """Maps and appends data based on column mapping"""
    # Use configured default bond type if not provided
    if default_bond_type is None:
        default_bond_type = config_manager.config["global"]["default_bond_type"]
        
    # Create DataFrame with current target columns
    mapped_df = pd.DataFrame(columns=get_target_columns())
    
    # Rest of the function remains the same...
    # ... [keep all existing code]

    for new_col in final_df.columns:
        if new_col in column_mapping:
            source_col = column_mapping[new_col]
            try:
                if isinstance(source_col, int):
                    mapped_df[new_col] = source_df.iloc[:, source_col]
                else:
                    mapped_df[new_col] = source_df[source_col]
            except (KeyError, IndexError):
                mapped_df[new_col] = pd.NA
        else:
            # Handle special cases
            if new_col == "sheet":
                mapped_df[new_col] = sheet_name
            elif new_col == "HB/L NO":
                mapped_df[new_col] = source_df.get("HBL", pd.NA)
            elif new_col == "Bond or Non Bond":
                mapped_df[new_col] = default_bond_type
            else:
                mapped_df[new_col] = pd.NA

    # Align datatypes before concatenation
    for col in final_df.columns:
        if col in mapped_df:
            mapped_df[col] = mapped_df[col].astype(final_df[col].dtype, errors='ignore')

    # Append and return
    final_df = pd.concat([final_df, mapped_df], ignore_index=True)

    print(f"✅ Appended data from: {sheet_name} | Final shape: {final_df.shape}")
    print(mapped_df[["HBL", "Bond or Non Bond", "sheet", "Origin", "Delivery date"]].head())
    return final_df,mapped_df


# ========================
# EXPO PROCESSING (YOUR EXISTING CODE)
# ========================

def process_expo_file(uploaded_file):
    try:
        # Process Bond Sheet - use get_target_consignees()
        bond_df = pd.read_excel(uploaded_file, sheet_name='Bond')
        matched_bond, _ = filter_and_match_consignee(bond_df, get_target_consignees())
        print(f"🔍 Matched Bond Rows: {len(matched_bond)}")
        processed_bond = process_bond_sheet(matched_bond) if not matched_bond.empty else pd.DataFrame()

        # Process Non-Bond Sheet
        non_bond_df = pd.read_excel(uploaded_file, sheet_name='NON-Bond')
        matched_non_bond, _ = filter_and_match_consignee(non_bond_df, get_target_consignees())
        print(f"🔍 Matched Non-Bond Rows: {len(matched_non_bond)}")
        processed_non_bond = process_non_bond_sheet(matched_non_bond) if not matched_non_bond.empty else pd.DataFrame()

        # Process FCL Sheet
        fcl_df = pd.read_excel(uploaded_file, sheet_name='FCL1')
        matched_fcl, _ = filter_and_match_consignee(fcl_df, get_target_consignees())
        print(f"🔍 Matched FCL Rows: {len(matched_fcl)}")
        processed_fcl = process_fcl_sheet(matched_fcl) if not matched_fcl.empty else pd.DataFrame()

        # Combine all non-empty sheets
        combined_sheets = [df for df in [processed_bond, processed_non_bond, processed_fcl] if not df.empty]

        if not combined_sheets:
            st.warning("⚠️ No matching rows found across any Expo sheets.")
            return pd.DataFrame()

        final_df = pd.concat(combined_sheets, ignore_index=True)

        # Clean up data types - convert potential date columns
        date_cols = ['ETA', 'ATD', 'ATA', 'ETB', 'Estimated Clearance', 'Delivery date']
        for col in date_cols:
            if col in final_df.columns:
                # First try to convert to datetime
                final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
                # Then convert valid dates to date-only format
                final_df[col] = final_df[col].apply(lambda x: x.date() if pd.notna(x) else pd.NA)

        st.session_state.expo_data = final_df
        st.session_state.expo_processed = True

        # --- 💡 Summary Section ---
        st.subheader("📊 Expo Processing Summary")

        total_rows = len(final_df)
        sheet_counts = final_df['sheet'].value_counts()

        st.markdown(f"**✅ Total Rows Processed:** `{total_rows}`")

        cols = st.columns(2)
        with cols[0]:
            st.markdown("**📁 Record Counts by Sheet**")
            st.dataframe(sheet_counts.rename_axis("Sheet").reset_index(name="Records"), 
                         use_container_width=True)

        with cols[1]:
            st.markdown("**📅 ETA Date Ranges**")
            if 'ETA' in final_df.columns:
                eta_stats = final_df[['sheet', 'ETA']].dropna().groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
                st.dataframe(eta_stats, use_container_width=True)
            else:
                st.warning("ETA column not found.")

        # --- 💾 Download Button ---
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download Merged Expo Data",
            data=csv,
            file_name='expo_merged.csv',
            mime='text/csv',
            key='expo_download'
        )

        return final_df

    except Exception as e:
        st.error(f"❌ Error processing Expo file: {str(e)}")
        return pd.DataFrame()

def process_bond_sheet(df):
    # Get mappings from configuration instead of hardcoded
    column_mappings = config_manager.get_mappings("expo", "bond")
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())
    
    # Apply mappings
    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col]
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                print(f"⚠️ Mapping failed for {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "expo_bond"
            elif target_col == "HB/L NO" and "HBL" in df.columns:
                processed_df[target_col] = df["HBL"]
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = "Bond"
            else:
                processed_df[target_col] = pd.NA



    return processed_df



def process_non_bond_sheet(df):
    # Get mappings from configuration
    column_mappings = config_manager.get_mappings("expo", "non_bond")
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())

    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col]
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                print(f"⚠️ Mapping failed for {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "expo_nonbond"
            elif target_col == "HB/L NO" and "HBL" in df.columns:
                processed_df[target_col] = df["HBL"]
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = "Non Bond"
            else:
                processed_df[target_col] = pd.NA

    return processed_df



def process_fcl_sheet(df):
    # Get mappings from configuration
    column_mappings = config_manager.get_mappings("expo", "fcl")
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())

    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col]
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                print(f"⚠️ Mapping failed for {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "expo_fcl"
            elif target_col == "HB/L NO" and "HBL" in df.columns:
                processed_df[target_col] = df["HBL"]
            elif target_col == "Bond or Non Bond":
                processed_df[target_col] = "FCL"
            else:
                processed_df[target_col] = pd.NA

    return processed_df


# ========================
# MAERSK PROCESSING 
# ========================
def process_maersk_file(uploaded_file):
    try:
        st.write("📥 Reading sheets from uploaded file...")
        dsr_df = pd.read_excel(uploaded_file, sheet_name='DSR')
        archived_df = pd.read_excel(uploaded_file, sheet_name='Archieved')


        # Step 1: Filter DSR
        matched_dsr, _ = filter_and_match_consignee(dsr_df, get_target_consignees())
        st.write(f"🔎 Matched DSR Rows: {matched_dsr.shape[0]}")
        if matched_dsr.empty:
            st.warning("⚠️ No matching rows in DSR sheet. Skipping.")
            processed_dsr = pd.DataFrame(columns=get_target_columns())  # Changed
        else:
            processed_dsr = process_maersk_dsr(matched_dsr)

        # Step 2: Filter Archived
        matched_archived, _ = filter_and_match_consignee(archived_df, get_target_consignees())
        st.write(f"🔎 Matched Archived Rows: {matched_archived.shape[0]}")
        if matched_archived.empty:
            st.warning("⚠️ No matching rows in Archived sheet. Skipping.")
            processed_archived = pd.DataFrame(columns=get_target_columns())  # Changed
        else:
            processed_archived = process_maersk_archived(matched_archived)

        # Step 3: Merge and convert dates
        final_df = pd.concat([processed_dsr, processed_archived], ignore_index=True)
        st.write(f"🧩 Combined Final Rows: {final_df.shape[0]}")

        if final_df.empty:
            st.warning("⚠️ Final dataset is empty after processing. No data to show.")
            return pd.DataFrame()  # Returning an empty DataFrame early

        # Convert datetime columns to date
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        st.session_state.maersk_data = final_df
        st.session_state.maersk_processed = True

        # Step 4: Summary + Downloads
        show_maersk_summary(final_df)

        st.subheader("📥 Maersk Data Downloads")
        col1, col2 = st.columns(2)

        with col1:
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Merged Maersk Data",
                data=csv,
                file_name='maersk_combined.csv',
                mime='text/csv',
                key='maersk_download'
            )

        with col2:
            if st.session_state.expo_processed:
                combined = pd.concat([st.session_state.expo_data, final_df], ignore_index=True)
                csv = combined.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Expo+Maersk Combined",
                    data=csv,
                    file_name='expo_maersk_combined.csv',
                    mime='text/csv',
                    key='expo_maersk_download'
                )
            else:
                st.warning("Expo data not processed yet")

        return final_df

    except Exception as e:
        st.error(f"❌ Maersk processing error: {str(e)}")
        print("❌ ERROR TRACE:", e)
        return pd.DataFrame()


def process_maersk_dsr(df):
    # Get mappings from config instead of hardcoded
    column_mappings = config_manager.get_mappings("maersk", "dsr")
    default_bond_type = config_manager.config["global"]["default_bond_type"]
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())

    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col] if source_col < len(df.columns) else pd.NA
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                st.warning(f"⚠️ Failed to map {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "maersk_dsr"
            elif target_col == "HB/L NO":
                processed_df[target_col] = df.get("HBL", pd.NA)
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = default_bond_type  # From config
            else:
                processed_df[target_col] = pd.NA

    return processed_df



def process_maersk_archived(df):

    column_mappings = config_manager.get_mappings("maersk", "archived")
    default_bond_type = config_manager.config["global"]["default_bond_type"]
    
    processed_df = pd.DataFrame(columns=get_target_columns())
    
    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col] if source_col < len(df.columns) else pd.NA
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                st.warning(f"⚠️ Failed to map {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "maersk_archived"
            elif target_col == "HB/L NO":
                processed_df[target_col] = df.get("HBL", pd.NA)
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = pd.NA  # From config
            else:
                processed_df[target_col] = pd.NA

    return processed_df



def show_maersk_summary(final_df):
    st.subheader("📊 Maersk Processing Summary")
    
    total_rows = len(final_df)
    sheet_counts = final_df['sheet'].value_counts()

    # 👉 Ensure 'ETA' is in datetime format before grouping
    if 'ETA' in final_df.columns:
        try:
            final_df['ETA'] = pd.to_datetime(final_df['ETA'], errors='coerce')
        except Exception as e:
            st.warning(f"⚠️ Failed to convert ETA to datetime: {str(e)}")

    # Now safely aggregate
    if 'ETA' in final_df.columns and pd.api.types.is_datetime64_any_dtype(final_df['ETA']):
        eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
        
        st.markdown("**📅 Date Ranges**")
        st.dataframe(eta_stats, hide_index=True, use_container_width=True)
    else:
        st.warning("⚠️ ETA column missing or not in datetime format. Skipping date ranges.")

    cols = st.columns(3)
    cols[0].metric("Total Rows", total_rows)
    cols[1].metric("DSR Records", sheet_counts.get('maersk_dsr', 0))
    cols[2].metric("Archived Records", sheet_counts.get('maersk_archived', 0))


# ========================
# GLOBE ACTIVE PROCESSING 
# ========================

def process_globe_file(uploaded_file):
    try:
        st.write("📥 Reading sheets from uploaded file...")
        ongoing_df = pd.read_excel(uploaded_file, sheet_name='ONGOING')
        cleared_df = pd.read_excel(uploaded_file, sheet_name='CLEARED')

        # Get configurations
        target_consignees = get_target_consignees()
        ongoing_mappings = config_manager.get_mappings("globe", "ongoing")
        cleared_mappings = config_manager.get_mappings("globe", "cleared")
        default_bond_type = config_manager.config["global"]["default_bond_type"]

        # Rest of your processing code...
        # Initialize final DataFrame
        final_df = pd.DataFrame(columns=get_target_columns())
        
        # Process ONGOING sheet - MODIFIED SECTION
        st.write("🔎 Processing ONGOING sheet...")
        ongoing_matched, _ = process_consignee_matching(
            df=ongoing_df,
            target_consignees=target_consignees,
            date_column="ETA DATE",
            consignee_column="CONSIGNEE"
        )
        
        if not ongoing_matched.empty:
            # Remove temporary columns added by process_consignee_matching
            ongoing_matched = ongoing_matched.drop(columns=['Consignee_clean', 'BestMatch', 'Score', 'ETA Remark'], errors='ignore')
            # Restore original column names
            ongoing_matched = ongoing_matched.rename(columns={
                'ETA': 'ETA DATE',
                'Consignee': 'CONSIGNEE'
            })
            
            final_df, mapped_ongoing = map_and_append_maersk_data(
                source_df=ongoing_matched,
                final_df=final_df,
                column_mapping=ongoing_mappings,
                sheet_name="globe_ongoing",
                # default_bond_type="FCL"
            )
            st.write(f"✅ Added {len(mapped_ongoing)} ONGOING records")
        else:
            st.warning("⚠️ No matching ONGOING records found")

        # Process CLEARED sheet - SAME MODIFICATION
        st.write("🔎 Processing CLEARED sheet...")
        cleared_matched, _ = process_consignee_matching(
            df=cleared_df,
            target_consignees=target_consignees,
            date_column="ETA DATE",
            consignee_column="CONSIGNEE"
        )
        
        if not cleared_matched.empty:
            cleared_matched = cleared_matched.drop(columns=['Consignee_clean', 'BestMatch', 'Score', 'ETA Remark'], errors='ignore')
            cleared_matched = cleared_matched.rename(columns={
                'ETA': 'ETA DATE',
                'Consignee': 'CONSIGNEE'
            })
            
            final_df, mapped_cleared = map_and_append_maersk_data(
                source_df=cleared_matched,
                final_df=final_df,
                column_mapping=cleared_mappings,
                sheet_name="globe_cleared",
            )
            st.write(f"✅ Added {len(mapped_cleared)} CLEARED records")
        else:
            st.warning("⚠️ No matching CLEARED records found")

        # Convert datetime columns
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        # Store in session state
        st.session_state.globe_data = final_df
        st.session_state.globe_processed = True

        # Show summary
        show_globe_summary(final_df)

        # Download options
        st.subheader("📥 Globe Active Data Downloads")
        col1, col2 = st.columns(2)
        
        with col1:
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Globe Data",
                data=csv,
                file_name='globe_active.csv',
                mime='text/csv',
                key='globe_download'
            )
        
        with col2:
            if st.session_state.expo_processed and st.session_state.maersk_processed:
                combined = pd.concat([
                    st.session_state.expo_data,
                    st.session_state.maersk_data,
                    final_df
                ], ignore_index=True)
                csv = combined.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Combined Data",
                    data=csv,
                    file_name='all_combined.csv',
                    mime='text/csv',
                    key='combined_download'
                )
            else:
                st.warning("Requires processed Expo and Maersk data")

        return final_df

    except Exception as e:
        st.error(f"❌ Globe Active processing error: {str(e)}")
        return pd.DataFrame()

def show_globe_summary(final_df):
    st.subheader("📊 Globe Active Summary")
    
    if final_df.empty:
        st.warning("No data available")
        return
    
    # Metrics
    total = len(final_df)
    ongoing = len(final_df[final_df['sheet'] == 'globe_ongoing'])
    cleared = len(final_df[final_df['sheet'] == 'globe_cleared'])
    
    cols = st.columns(3)
    cols[0].metric("Total Records", total)
    cols[1].metric("Ongoing Shipments", ongoing)
    cols[2].metric("Cleared Shipments", cleared)
    
    # Date ranges
    st.write("\n")
    st.markdown("**📅 ETA Date Ranges**")
    eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
    st.dataframe(eta_stats, use_container_width=True)

# ========================
# SCANWELL PROCESSING 
# ========================
def process_scanwell_file(uploaded_file):
    try:
        st.write("📥 Reading sheets from uploaded file...")
        unichela_df = pd.read_excel(uploaded_file, sheet_name='UNICHELA -2025')
        bodyline_df = pd.read_excel(uploaded_file, sheet_name='BODYLINE-2025')

        target_consignees = get_target_consignees()
        unichela_mappings = config_manager.get_mappings("scanwell", "unichela")
        bodyline_mappings = config_manager.get_mappings("scanwell", "bodyline")

        # Initialize final DataFrame
        final_df = pd.DataFrame(columns=get_target_columns())
        
     
        # Process UNICHELA sheet
        st.write("🔎 Processing UNICHELA sheet...")
        unichela_matched, _ = process_consignee_matching(
            df=unichela_df,
            target_consignees=target_consignees,
            date_column="ETA",
            consignee_column="Consignee"
        )
        
        if not unichela_matched.empty:
            final_df, mapped_unichela = map_and_append_maersk_data(
                source_df=unichela_matched,
                final_df=final_df,
                column_mapping=unichela_mappings,
                sheet_name="scanwell_unichela"
            )
            st.write(f"✅ Added {len(mapped_unichela)} UNICHELA records")
        else:
            st.warning("⚠️ No matching UNICHELA records found")

        # Process BODYLINE sheet
        st.write("🔎 Processing BODYLINE sheet...")
        bodyline_matched, _ = process_consignee_matching(
            df=bodyline_df,
            target_consignees=target_consignees,
            date_column="ETA",
            consignee_column="Consignee"
        )
        
        if not bodyline_matched.empty:
            final_df, mapped_bodyline = map_and_append_maersk_data(
                source_df=bodyline_matched,
                final_df=final_df,
                column_mapping=bodyline_mappings,
                sheet_name="scanwell_bodyline"
            )
            st.write(f"✅ Added {len(mapped_bodyline)} BODYLINE records")
        else:
            st.warning("⚠️ No matching BODYLINE records found")

        # Convert datetime columns
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        # Store in session state
        st.session_state.scanwell_data = final_df
        st.session_state.scanwell_processed = True

        # Show summary
        show_scanwell_summary(final_df)

        # Download options
        st.subheader("📥 Scanwell Data Downloads")
        col1, col2 = st.columns(2)
        
        with col1:
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Scanwell Data",
                data=csv,
                file_name='scanwell_combined.csv',
                mime='text/csv',
                key='scanwell_download'
            )
        
        with col2:
            if st.session_state.expo_processed and st.session_state.maersk_processed and st.session_state.globe_processed:
                combined = pd.concat([
                    st.session_state.expo_data,
                    st.session_state.maersk_data,
                    st.session_state.globe_data,
                    final_df
                ], ignore_index=True)
                csv = combined.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Combined Data",
                    data=csv,
                    file_name='all_combined.csv',
                    mime='text/csv',
                    key='combined_download'
                )
            else:
                st.warning("Requires processed Expo, Maersk and Globe data")

        return final_df

    except Exception as e:
        st.error(f"❌ Scanwell processing error: {str(e)}")
        return pd.DataFrame()

def show_scanwell_summary(final_df):
    st.subheader("📊 Scanwell Summary")
    
    if final_df.empty:
        st.warning("No data available")
        return
    
    # Metrics
    total = len(final_df)
    unichela = len(final_df[final_df['sheet'] == 'scanwell_unichela'])
    bodyline = len(final_df[final_df['sheet'] == 'scanwell_bodyline'])
    
    cols = st.columns(3)
    cols[0].metric("Total Records", total)
    cols[1].metric("Unichela Shipments", unichela)
    cols[2].metric("Bodyline Shipments", bodyline)
    
    # Date ranges
    st.write("\n")
    st.markdown("**📅 ETA Date Ranges**")
    eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
    st.dataframe(eta_stats, use_container_width=True)

# ========================
# UI COMPONENTS
# ========================

import streamlit as st
import pandas as pd
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import re

def process_dsr_merchant_data_original(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform DSR merchant data for standardized output."""

    # --- Helper Functions ---
    def clean_status(status, sheet):
        if pd.isna(status) and sheet.lower() == 'expo_fcl':
            return 'FCL'
        status = str(status).strip().upper()
        return status if status in ['FCL', 'LCL'] else np.nan

    def clean_bond_status(row):
        val = str(row['Bond or Non Bond']).strip().upper().replace('-', '').replace('_', '').replace(' ', '')
        if val == 'BOND':
            return 'BOND'
        elif val in ['NONBOND', 'FCL']:
            return 'NON-BOND'
        if 'maersk' in row['sheet'].lower() or 'globe' in row['sheet'].lower():
            return 'NON-BOND'
        if 'scanwell' in row['sheet'].lower() and isinstance(row['Port'], str):
            if any(port in row['Port'].upper() for port in ['SHENZHEN', 'HKG']):
                return 'BOND'
        return np.nan

    def fill_origin_vessel(row):
        return row['Connecting vessel'] if pd.isna(row['Origin Vessel']) and pd.notna(row['Connecting vessel']) else row['Origin Vessel']

    def clean_vessel_name(name):
        if not isinstance(name, str):
            return ""
        name = re.sub(r'[\/"\'\.]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        name = re.sub(r'(\d{2,})(\1)', r'\1', name)
        return name.upper()

    def group_similar_names(names, threshold=80):
        visited = set()
        groups = []
        for i, base in enumerate(names):
            if base in visited:
                continue
            group = [base]
            visited.add(base)
            for candidate in names[i+1:]:
                if candidate not in visited and fuzz.token_set_ratio(base, candidate) >= threshold:
                    group.append(candidate)
                    visited.add(candidate)
            if len(group) > 1:
                groups.append(group)
        return groups

    def compute_clearance(row):
        if pd.isna(row['ATA']):
            return pd.NaT
        status = str(row.get('LCL,FCL Status', '')).strip().upper()
        days = 5 if status == 'LCL' else 3 if status == 'FCL' else None
        return row['ATA'] + pd.Timedelta(days=days) if days else pd.NaT

    # --- Cleaning Steps ---
    df = df.copy()

    # Clean LCL/FCL status
    df['LCL,FCL Status'] = df.apply(lambda row: clean_status(row['LCL,FCL Status'], row['sheet']), axis=1)

    # Clean Bond/Non-Bond
    df['Bond or Non Bond'] = df.apply(clean_bond_status, axis=1)

    # Fill missing Origin Vessel
    df['Origin Vessel'] = df.apply(fill_origin_vessel, axis=1)

    # Combine and clean vessel+voyage info
    df['Vessel_Voyage'] = df['Origin Vessel'].fillna('').astype(str) + ' ' + df['Voyage No'].fillna('').astype(str)
    df['Vessel_Voyage_Cleaned'] = df['Vessel_Voyage'].apply(clean_vessel_name)

    # Standardize vessel names
    unique_cleaned = df['Vessel_Voyage_Cleaned'].dropna().unique()
    groups = group_similar_names(list(unique_cleaned))
    name_map = {variant: group[0] for group in groups for variant in group}
    df['Vessel_Voyage_Standard'] = df['Vessel_Voyage_Cleaned'].map(name_map).fillna(df['Vessel_Voyage_Cleaned'])

    # Move standardized vessel before 'Origin Vessel'
    cols = df.columns.tolist()
    if 'Vessel_Voyage_Standard' in cols and 'Origin Vessel' in cols:
        cols.insert(cols.index('Origin Vessel'), cols.pop(cols.index('Vessel_Voyage_Standard')))
        df = df[cols]

    # Process ETA, ATD, ATA, ETB
    for col in ['ETA', 'ATD']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'ETA' in df.columns and 'ATD' in df.columns:
        eta_col = df.pop('ETA')
        df.insert(df.columns.get_loc('ATD') + 1, 'ETA', eta_col)

    df['ATA'] = df['ETA'] + pd.Timedelta(days=1)
    df['ETB'] = df['ATA'] + pd.Timedelta(days=1)
    df['Estimated Clearance'] = df.apply(compute_clearance, axis=1)

    return df

import pdfplumber
from rapidfuzz import fuzz, process
from datetime import datetime, timedelta

def extract_pdf_tables(pdf_path: str) -> pd.DataFrame:
    """Extract and standardize tables from PDF meeting minutes"""
    required_columns = [
        'VESSEL & VOYAGE', 'ORIGIN', 'ETD', 'BOOKING ETA', 'FORECAST ETA', 'CURRENT STATUS'
    ]
    
    def normalize_column(name):
        if name is None:
            return ""
        return (
            str(name).strip()
            .lower()
            .replace('\u200b', '')
            .replace('\xa0', ' ')
            .replace('\n', ' ')
        )

    def map_columns_with_similarity(actual_columns, expected_columns, fuzzy_threshold=85):
        mapped = {}
        normalized_actual = {normalize_column(col): col for col in actual_columns}

        for expected_col in expected_columns:
            norm_expected = normalize_column(expected_col)

            # Exact match
            if norm_expected in normalized_actual:
                original_col = normalized_actual[norm_expected]
                mapped[original_col] = expected_col
                continue

            # Fuzzy fallback
            match, score, _ = process.extractOne(
                norm_expected, normalized_actual.keys(), scorer=fuzz.token_sort_ratio
            )
            if score >= fuzzy_threshold:
                original_col = normalized_actual[match]
                mapped[original_col] = expected_col

        return mapped

    def clean_and_merge_tables(tables, required_columns):
        clean_tables = []

        for i, table in enumerate(tables):
            if not isinstance(table, pd.DataFrame) or table.empty:
                continue

            # Handle 'ORIGIN' and 'Loading Port' columns
            col_norm = [normalize_column(c) for c in table.columns]
            has_origin = any('origin' == c for c in col_norm)
            has_loading_port = any('loading port' == c for c in col_norm)

            if has_loading_port:
                loading_port_col = next(c for c in table.columns if normalize_column(c) == 'loading port')
                table['ORIGIN'] = table[loading_port_col]
            elif has_origin:
                origin_col = next(c for c in table.columns if normalize_column(c) == 'origin')
                table['ORIGIN'] = table[origin_col]

            mapping = map_columns_with_similarity(table.columns, required_columns)
            if not mapping:
                continue

            # Rename and filter
            table = table.rename(columns=mapping)
            valid_cols = [col for col in required_columns if col in table.columns]
            table = table[valid_cols]
            clean_tables.append(table)

        return pd.concat(clean_tables, ignore_index=True) if clean_tables else pd.DataFrame(columns=required_columns)

    # Extract tables from PDF
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            for table_data in page_tables:
                df = pd.DataFrame(table_data[1:], columns=table_data[0])
                tables.append(df)

    # Clean and merge tables
    merged_df = clean_and_merge_tables(tables, required_columns)
    
    # Clean the final dataframe
    if not merged_df.empty:
        merged_df = merged_df[merged_df['VESSEL & VOYAGE'].notna()]
        merged_df = merged_df[merged_df['VESSEL & VOYAGE'].str.strip() != '']
        
        # Clean vessel names
        merged_df['VESSEL_CLEAN'] = merged_df['VESSEL & VOYAGE'].apply(
            lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x)).strip().upper()
        )
    
    return merged_df


def match_vessels(processed_df: pd.DataFrame, pdf_df: pd.DataFrame) -> pd.DataFrame:
    """Enhanced vessel matching with robust error handling"""
    if pdf_df.empty or processed_df.empty:
        return processed_df
    
    # Ensure we have the required columns
    if 'VESSEL_CLEAN' not in pdf_df.columns or 'Vessel_Voyage_Standard' not in processed_df.columns:
        print("Missing required columns for matching")
        return processed_df
    
    # Get unique vessels from both sources
    pdf_vessels = [str(v).strip().upper() for v in pdf_df['VESSEL_CLEAN'].unique() if pd.notna(v)]
    processed_vessels = [str(v).strip().upper() for v in processed_df['Vessel_Voyage_Standard'].unique() if pd.notna(v)]
    
    print(f"\nMatching {len(processed_vessels)} processed vessels against {len(pdf_vessels)} PDF vessels")
    
    # Create mapping dictionary with similarity scores
    vessel_map = {}
    similarity_scores = {}
    
    for vessel in processed_vessels:
        try:
            result = process.extractOne(
                vessel, 
                pdf_vessels, 
                scorer=fuzz.token_sort_ratio,
                score_cutoff=60  # Lower threshold to catch more matches
            )
            
            if result is not None:
                match, score, _ = result
                if score >= 60:  # Only consider good enough matches
                    vessel_map[vessel] = match
                    similarity_scores[vessel] = score
        except Exception as e:
            print(f"Error matching vessel {vessel}: {str(e)}")
            continue
    
    print(f"\nFound {len(vessel_map)} matches")
    if vessel_map:
        print("Sample matches:")
        for vessel, match in list(vessel_map.items())[:5]:
            print(f"- {vessel} → {match} (score: {similarity_scores[vessel]})")
    
    # Add PDF data to processed dataframe
    processed_df['PDF_VESSEL_MATCH'] = processed_df['Vessel_Voyage_Standard'].str.strip().str.upper().map(vessel_map)
    processed_df['MATCH_SCORE'] = processed_df['Vessel_Voyage_Standard'].str.strip().str.upper().map(similarity_scores)
    
    return processed_df


def process_dsr_merchant_data(df: pd.DataFrame, pdf_path: str = None) -> pd.DataFrame:
    """Enhanced processing with PDF integration and debugging"""
    # Original processing steps
    df = process_dsr_merchant_data_original(df)
    
    # PDF extraction and matching if path provided
    if pdf_path:
        try:
            print("\nStarting PDF processing...")
            pdf_df = extract_pdf_tables(pdf_path)
            
            if not pdf_df.empty:
                print("PDF data successfully extracted")
                df = match_vessels(df, pdf_df)
                
                # Enhanced timeline processing with PDF data
                if 'BOOKING ETA' in df.columns:
                    df['ETA'] = pd.to_datetime(df['BOOKING ETA'], errors='coerce')
                if 'FORECAST ETA' in df.columns:
                    df['ETA'] = df['ETA'].fillna(pd.to_datetime(df['FORECAST ETA'], errors='coerce'))
                
                # Recalculate timelines
                df['ATA'] = df['ETA'] + timedelta(days=1)
                df['ETB'] = df['ATA'] + timedelta(days=1)
                df['Estimated Clearance'] = df.apply(compute_clearance, axis=1)
                
                print("PDF processing completed successfully")
                
            else:
                print("No data extracted from PDF")
                
        except Exception as e:
            print(f"PDF processing error: {str(e)}")
            st.error(f"PDF processing error: {str(e)}")
    
    return df


def compute_clearance(row):
    if pd.isna(row['ATA']):
        return pd.NaT
    status = str(row.get('LCL,FCL Status', '')).strip().upper()
    days = 5 if status == 'LCL' else 3 if status == 'FCL' else None
    return row['ATA'] + pd.Timedelta(days=days) if days else pd.NaT




def show_current_step():
    st.title("📊 Piyadasa-Import Shipment Tracker")
    st.caption("Process and combine shipment data from multiple sources")

    step = st.selectbox("🔢 Jump to Step", [1, 2, 3, 4, 5], index=st.session_state.current_step - 1)
    if step != st.session_state.current_step:
        st.session_state.current_step = step
        st.rerun()


  
    current_step = st.session_state.current_step

    # Step 1: Expo Data
    if current_step == 1:
        with st.container(border=True):
            st.subheader("1. Upload Expo Data")
            uploaded_file = st.file_uploader(
                "Select Expo Excel File", 
                type=['xlsx'],
                key='expo_upload',
                help="Upload the DSR MAS Excel file with Bond/NON-Bond/FCL sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Expo data...'):
                    result = process_expo_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Expo data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ Continue to Maersk Data", type="primary"):
                            st.session_state.current_step = 2
                            st.rerun()
    
    # Step 2: Maersk Data
    elif current_step == 2:
        with st.container(border=True):
            st.subheader("2. Upload Maersk Data")
            uploaded_file = st.file_uploader(
                "Select Maersk Excel File", 
                type=['xlsx'],
                key='maersk_upload',
                help="Upload the Maersk Excel file with DSR/Archived sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Maersk data...'):
                    result = process_maersk_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Maersk data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ Continue to Globe Data", type="primary"):
                            st.session_state.current_step = 3
                            st.rerun()
    
    # Step 3: Globe Data
    elif current_step == 3:
        with st.container(border=True):
            st.subheader("3. Upload Globe Active Data")
            uploaded_file = st.file_uploader(
                "Select Globe Excel File", 
                type=['xlsx'],
                key='globe_upload',
                help="Upload the Globe Active Excel file with ONGOING/CLEARED sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Globe data...'):
                    result = process_globe_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Globe data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ Continue to Scanwell Data", type="primary"):
                            st.session_state.current_step = 4
                            st.rerun()
    
    # Step 4: Scanwell Data
    elif current_step == 4:
        with st.container(border=True):
            st.subheader("4. Upload Scanwell Data")
            uploaded_file = st.file_uploader(
                "Select Scanwell Excel File", 
                type=['xls', 'xlsx'],
                key='scanwell_upload',
                help="Upload the Scanwell Excel file with UNICHELA/BODYLINE sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Scanwell data...'):
                    result = process_scanwell_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Scanwell data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ View Final Results", type="primary"):
                            st.session_state.current_step = 5
                            st.rerun()
    # Final Step: Results
    elif current_step == 5:
        with st.container(border=True):
            st.subheader("📊 Data Visualization & Analysis")
            
            analysis_tab, consolidated_tab = st.tabs(["🔍 Data Analysis", "🚢 Consolidated Shipments"])

            with analysis_tab:

                # Initialize combined as empty DataFrame
                combined = pd.DataFrame()
                
                # Add option to use existing merged data, upload new, or fetch from Google Sheets
                analysis_option = st.radio(
                    "Choose data source:",
                    options=["Fetch from Google Sheets", "Use previously processed data", "Upload new file for analysis"],
                    index=0,  # Default to Google Sheets
                    horizontal=True
                )
                
                if analysis_option == "Fetch from Google Sheets":
                    try:
                        import requests
                        from io import BytesIO
                        import urllib3
                        
                        # Disable SSL verification warnings (not recommended for production)
                        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                        
                        # Google Sheets URL (published to web as XLSX)
                        google_sheets_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRuIk9kF1gp-TlmhHpPkcC75IeWFm4r_QTYtJOePHa0ZcoEaVvUmJiHL6i0R2282YjKfuBwu7-B1CNt/pub?output=xlsx'
                        
                        # Read data from Google Sheets with SSL verification disabled
                        response = requests.get(google_sheets_url, verify=False)
                        combined = pd.read_excel(BytesIO(response.content))
                        st.success("✅ Data fetched from Google Sheets successfully!")
                        
                    except Exception as e:
                        st.error(f"Failed to fetch data from Google Sheets: {str(e)}")

            
                elif analysis_option == "Upload new file for analysis":
                    quick_file = st.file_uploader(
                        "Upload any Excel/CSV for analysis",
                        type=['xlsx', 'xls', 'csv'],
                        key='quick_analysis'
                    )
                    
                    if quick_file:
                        try:
                            if quick_file.name.endswith('.csv'):
                                combined = pd.read_csv(quick_file)
                            else:
                                combined = pd.read_excel(quick_file)
                            #st.success("File uploaded successfully!")
                        except Exception as e:
                            st.error(f"Error reading file: {str(e)}")
                else:
                    # FLEXIBLE MERGING: Use any available processed data
                    data_sources = []

                    if st.session_state.get('expo_processed'):
                        data_sources.append(st.session_state.expo_data)

                    if st.session_state.get('maersk_processed'):
                        data_sources.append(st.session_state.maersk_data)

                    if st.session_state.get('globe_processed'):
                        data_sources.append(st.session_state.globe_data)

                    if st.session_state.get('scanwell_processed'):
                        data_sources.append(st.session_state.scanwell_data)

                    if data_sources:
                        combined = pd.concat(data_sources, ignore_index=True)
                    else:
                        st.warning("⚠️ No processed data available - please upload files in previous steps")

                # Only proceed if we have data
                if not combined.empty:
                    st.success(f"✨ Data loaded! Total records: {len(combined):,}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Unique HBLs", combined['HBL'].nunique())
                    with col2:
                        # Normalize column names
                        combined.columns = [col.strip() for col in combined.columns]
                        gross_weight_col = next(
                            (col for col in combined.columns if col.lower().replace(" ", "") == "grossweight"), 
                            None
                        )
                        if gross_weight_col:
                            try:
                                combined[gross_weight_col] = (
                                    pd.to_numeric(
                                        combined[gross_weight_col]
                                        .astype(str)
                                        .str.replace(",", "")
                                        .str.extract(r"(\d+\.?\d*)")[0],
                                        errors='coerce'
                                    )
                                )
                                total_weight = combined[gross_weight_col].sum()
                                st.metric("Total Gross Weight", f"{total_weight:,.2f} kg")
                            except Exception as e:
                                st.metric("Total Gross Weight", f"Error: {str(e)}")
                        else:
                            st.metric("Total Gross Weight", "Column not found")

                    # Filter block
                    st.markdown("### 🔎 Filter Records")
                    filter_col1, filter_col2 = st.columns(2)

                    with filter_col1:
                        selected_hbls = st.multiselect(
                            "Filter by HBL",
                            options=combined['HBL'].dropna().unique(),
                            default=None
                        )

                    with filter_col2:
                        selected_invs = st.multiselect(
                            "Filter by Inv #",
                            options=combined['Inv #'].dropna().unique(),
                            default=None
                        )

                    filtered_combined = combined.copy()
                    if selected_hbls:
                        filtered_combined = filtered_combined[filtered_combined['HBL'].isin(selected_hbls)]
                    if selected_invs:
                        filtered_combined = filtered_combined[filtered_combined['Inv #'].isin(selected_invs)]

                    st.info(f"🔍 Showing {len(filtered_combined):,} filtered records")

                    # Show filtered data
                    if not filtered_combined.empty:
                        with st.expander("🔍 View Filtered Data", expanded=False):
                            st.dataframe(
                                filtered_combined,
                                use_container_width=True,
                                height=400
                            )

                    # Visualizations organized in tabs
                    st.markdown("---")
                    st.subheader("📊 Additional Visual Insights")
                    
                    # Initialize date filter variables
                    viz_filtered = filtered_combined.copy()
                    
                    if 'ETA' in filtered_combined.columns:
                        try:
                            # Ensure ETA is datetime and drop NA values
                            filtered_combined['ETA'] = pd.to_datetime(filtered_combined['ETA'], errors='coerce')
                            valid_dates = filtered_combined.dropna(subset=['ETA'])
                            
                            if not valid_dates.empty:
                                min_date = valid_dates['ETA'].min().date()
                                max_date = valid_dates['ETA'].max().date()
                                

                                if 'date_filter' not in st.session_state:
                                    default_start = max(min_date, max_date - timedelta(days=30))
                                    st.session_state.date_filter = {
                                        'start_date': default_start,
                                        'end_date': max_date
                                    }
                                    st.session_state.pop('viz_start_date', None)
                                    st.session_state.pop('viz_end_date', None)

                                # Date filter section for visualizations
                                st.markdown("### ⏳ Filter Visualizations by Date Range")
                                date_col1, date_col2, date_col3 = st.columns([2, 2, 1])
                                
                                date_key_suffix = f"{min_date}_{max_date}"

                                with date_col1:
                                    start_date = st.date_input(
                                        "Start Date",
                                        value=st.session_state.date_filter['start_date'],
                                        min_value=min_date,
                                        max_value=max_date,
                                        key=f'viz_start_date_{date_key_suffix}'
                                    )

                                with date_col2:
                                    end_date = st.date_input(
                                        "End Date",
                                        value=st.session_state.date_filter['end_date'],
                                        min_value=min_date,
                                        max_value=max_date,
                                        key=f'viz_end_date_{date_key_suffix}'
                                    )

                                
                                with date_col3:
                                    st.write("")  # Spacer for alignment
                                    if st.button("🔄 Reset Dates", key="reset_viz_dates"):
                                        st.session_state.date_filter = {
                                            'start_date': min_date,
                                            'end_date': max_date
                                        }
                                        st.rerun()
                                
                                # Update session state if dates changed
                                if (start_date != st.session_state.date_filter['start_date'] or 
                                    end_date != st.session_state.date_filter['end_date']):
                                    st.session_state.date_filter = {
                                        'start_date': start_date,
                                        'end_date': end_date
                                    }
                                    st.rerun()
                                
                                # Apply date filter to all visualizations
                                viz_filtered = filtered_combined[
                                    (filtered_combined['ETA'].dt.date >= st.session_state.date_filter['start_date']) & 
                                    (filtered_combined['ETA'].dt.date <= st.session_state.date_filter['end_date'])
                                ]
                                
                                # Show record count instead of date range
                                st.info(f"📊 Showing {len(viz_filtered):,} records")
                            else:
                                st.warning("No valid dates available in ETA column")
                                viz_filtered = filtered_combined
                                
                        except Exception as e:
                            st.warning(f"Couldn't process dates: {e}")
                            viz_filtered = filtered_combined
                    else:
                        viz_filtered = filtered_combined
                    
                    # Visualization tabs - ALL using viz_filtered with proper formatting
                    tab1, tab2, tab3, tab4 = st.tabs([
                        "📅 Time Trends", 
                        "🚢 Vessel Insights", 
                        "🏢 SBU Insights", 
                        "🌍 Origin Insights"
                    ])

                    with tab1:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if 'ETA' in viz_filtered.columns and 'sheet' in viz_filtered.columns:
                                try:
                                    eta_df = viz_filtered.dropna(subset=['ETA', 'sheet'])
                                    if not eta_df.empty:
                                        eta_df['ETA_Date'] = eta_df['ETA'].dt.date
                                        eta_trend_df = eta_df.groupby(['sheet', 'ETA_Date']).size().reset_index(name='Bookings')
                                        
                                        fig_eta = px.line(
                                            eta_trend_df,
                                            x='ETA_Date',
                                            y='Bookings',
                                            color='sheet',
                                            title='📆 Bookings Over Time by Sheet',
                                            markers=True,
                                            labels={'ETA_Date': 'ETA Date', 'Bookings': 'Number of Bookings'}
                                        )
                                        fig_eta.update_layout(
                                            paper_bgcolor='rgba(0,0,0,0)',
                                            plot_bgcolor='rgba(0,0,0,0)',
                                            font=dict(color='white' if st.session_state.dark_mode else 'black'),
                                            height=500
                                        )
                                        st.plotly_chart(fig_eta, use_container_width=True)
                                    else:
                                        st.warning("No data available after filtering")
                                except Exception as e:
                                    st.warning(f"Couldn't generate ETA trend chart: {e}")
                        
                        with col2:
                            if 'Delivery date' in viz_filtered.columns:
                                try:
                                    delivery_df = viz_filtered.copy()
                                    
                                    # Exclude 'globe_cleared' rows if the column exists
                                    if 'globe_cleared' in delivery_df.columns:
                                        delivery_df = delivery_df[~delivery_df['globe_cleared'].notna()]
                                    
                                    # Determine delivery status
                                    delivery_df['Status'] = 'Pending'  # Default to pending
                                    delivery_df.loc[delivery_df['Delivery date'].notna(), 'Status'] = 'Delivered'
                                    
                                    # Include 'globe_ongoing' in pending if the column exists
                                    if 'globe_ongoing' in delivery_df.columns:
                                        delivery_df.loc[delivery_df['globe_ongoing'].notna(), 'Status'] = 'Pending'
                                    
                                    # Count statuses
                                    delivery_summary = delivery_df['Status'].value_counts().reset_index()
                                    delivery_summary.columns = ['Status', 'Count']
                                    
                                    # Create the plot
                                    fig_delivery = px.bar(
                                        delivery_summary,
                                        x='Status',
                                        y='Count',
                                        color='Status',
                                        title='🚚 Delivery Status Breakdown',
                                        text='Count',
                                        category_orders={"Status": ["Delivered", "Pending"]}  # Ensures consistent order
                                    )
                                    
                                    fig_delivery.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black'),
                                        xaxis_title="Delivery Status",
                                        yaxis_title="Number of Orders"
                                    )
                                    
                                    # Update color mapping if needed
                                    color_map = {'Delivered': 'green', 'Pending': 'orange'}
                                    fig_delivery.for_each_trace(lambda t: t.update(marker_color=color_map[t.name]))
                                    
                                    st.plotly_chart(fig_delivery, use_container_width=True)
                                    
                                except Exception as e:
                                    st.warning(f"Couldn't generate delivery status chart: {e}")
                    with tab2:
                        # Vessel insights using viz_filtered with proper formatting
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if 'Origin Vessel' in viz_filtered.columns:
                                try:
                                    vessel_counts = viz_filtered['Origin Vessel'].value_counts().head(10).reset_index()
                                    vessel_counts.columns = ['Vessel', 'Count']
                                    fig_vessel = px.bar(
                                        vessel_counts,
                                        x='Vessel',
                                        y='Count',
                                        color='Vessel',
                                        title='Top 10 Vessels by Shipment Count'
                                    )
                                    fig_vessel.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black')
                                    )
                                    st.plotly_chart(fig_vessel, use_container_width=True)
                                except Exception as e:
                                    st.warning(f"Couldn't generate vessel chart: {e}")
                        
                        with col2:
                            if 'Origin Vessel' in viz_filtered.columns and 'Gross Weight' in viz_filtered.columns:
                                try:
                                    vessel_weight = viz_filtered.groupby('Origin Vessel')['Gross Weight'].sum().head(6).reset_index()
                                    vessel_weight.columns = ['Vessel', 'Total Weight']
                                    fig_weight = px.pie(
                                        vessel_weight,
                                        names='Vessel',
                                        values='Total Weight',
                                        title='Weight Distribution by Vessel (Top 6)'
                                    )
                                    fig_weight.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black')
                                    )
                                    st.plotly_chart(fig_weight, use_container_width=True)
                                except Exception as e:
                                    st.warning(f"Couldn't generate weight chart: {e}")

                    with tab3:
                        # SBU insights using viz_filtered with proper formatting
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if 'SBU' in viz_filtered.columns:
                                try:
                                    sbu_counts = viz_filtered['SBU'].value_counts().reset_index()
                                    sbu_counts.columns = ['SBU', 'Count']
                                    fig_sbu = px.bar(
                                        sbu_counts,
                                        x='SBU',
                                        y='Count',
                                        color='SBU',
                                        title='Shipments by SBU'
                                    )
                                    fig_sbu.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black')
                                    )
                                    st.plotly_chart(fig_sbu, use_container_width=True)
                                except Exception as e:
                                    st.warning(f"Couldn't generate SBU chart: {e}")
                        
                        with col2:
                            if 'SBU' in viz_filtered.columns and 'Gross Weight' in viz_filtered.columns:
                                try:
                                    sbu_weight = viz_filtered.groupby('SBU')['Gross Weight'].sum().reset_index()
                                    sbu_weight.columns = ['SBU', 'Total Weight']
                                    fig_sbu_weight = px.treemap(
                                        sbu_weight,
                                        path=['SBU'],
                                        values='Total Weight',
                                        title='Weight Distribution by SBU'
                                    )
                                    fig_sbu_weight.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black')
                                    )
                                    st.plotly_chart(fig_sbu_weight, use_container_width=True)
                                except Exception as e:
                                    st.warning(f"Couldn't generate SBU weight chart: {e}")

                    with tab4:
                        # Origin insights using viz_filtered with proper formatting
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if 'Origin' in viz_filtered.columns:
                                try:
                                    origin_counts = viz_filtered['Origin'].value_counts().head(10).reset_index()
                                    origin_counts.columns = ['Origin', 'Count']
                                    fig_origin = px.bar(
                                        origin_counts,
                                        x='Origin',
                                        y='Count',
                                        color='Origin',
                                        title='Top 10 Origins by Shipment Count'
                                    )
                                    fig_origin.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black')
                                    )
                                    st.plotly_chart(fig_origin, use_container_width=True)
                                except Exception as e:
                                    st.warning(f"Couldn't generate origin chart: {e}")
                        
                        with col2:
                            if 'Origin' in viz_filtered.columns and 'Shipper' in viz_filtered.columns:
                                try:
                                    origin_shipper = viz_filtered.groupby(['Origin', 'Shipper']).size().reset_index(name='Count')
                                    fig_origin_shipper = px.sunburst(
                                        origin_shipper,
                                        path=['Origin', 'Shipper'],
                                        values='Count',
                                        title='Shipper Distribution by Origin'
                                    )
                                    fig_origin_shipper.update_layout(
                                        height=500,
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black')
                                    )
                                    st.plotly_chart(fig_origin_shipper, use_container_width=True)
                                except Exception as e:
                                    st.warning(f"Couldn't generate origin-shipper chart: {e}")


            with consolidated_tab:
                st.subheader("🌊✈️ Merchant Sending DSR View")
                
                # PDF Upload Section
                uploaded_pdf = st.file_uploader(
                    "Upload Operation Meeting Minutes PDF (Optional)", 
                    type=['pdf'],
                    key='dsr_pdf'
                )
                
                if not combined.empty:
                    # Show pre-processing insights
                    st.markdown("### 🔍 Pre-Processing Insights")
                    with st.expander("View raw data characteristics"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Data Shape**")
                            st.code(f"Rows: {len(combined)}\nColumns: {len(combined.columns)}")
                            
                            st.write("**Missing Values**")
                            missing_counts = combined.isna().sum().sort_values(ascending=False)
                            st.dataframe(missing_counts.rename("Missing Count"), height=200)
                        
                        with col2:
                            st.write("**Key Columns**")
                            key_cols = ['HBL', 'Origin Vessel', 'Voyage No', 'LCL,FCL Status', 'Bond or Non Bond']
                            present_cols = [c for c in key_cols if c in combined.columns]
                            st.write(", ".join(present_cols) or "None available")
                            
                            if 'Origin Vessel' in combined.columns:
                                vessel_variations = combined['Origin Vessel'].nunique()
                                st.metric("Unique Vessel Names (Raw)", vessel_variations)

                    # Process the data
                    # After processing the data
                    with st.spinner("Standardizing DSR merchant data..."):
                        if uploaded_pdf:
                            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                tmp_file.write(uploaded_pdf.read())
                                pdf_path = tmp_file.name
                            processed_df = process_dsr_merchant_data(combined, pdf_path)
                            
                            # DEBUG: Show matching information
                            with st.expander("Debug: Vessel Matching Details", expanded=False):
                                if 'PDF_VESSEL_MATCH' in processed_df.columns:
                                    matches = processed_df[['Vessel_Voyage_Standard', 'PDF_VESSEL_MATCH', 'MATCH_SCORE']].dropna()
                                    st.write(f"Found {len(matches)} vessel matches")
                                    st.dataframe(matches)
                                    
                                    # Show unmatched vessels
                                    unmatched = processed_df[processed_df['PDF_VESSEL_MATCH'].isna()]
                                    st.write(f"{len(unmatched)} vessels couldn't be matched")
                                    if not unmatched.empty:
                                        st.dataframe(unmatched['Vessel_Voyage_Standard'].value_counts().head(10))
                                else:
                                    st.warning("No PDF_VESSEL_MATCH column created")
                        else:
                            processed_df = process_dsr_merchant_data_original(combined)
                    
                    # Show post-processing insights
                    st.markdown("---")
                    st.markdown("### ✅ Processing Results")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if 'Vessel_Voyage_Standard' in processed_df.columns:
                            orig_vessels = combined['Origin Vessel'].nunique() if 'Origin Vessel' in combined.columns else 0
                            std_vessels = processed_df['Vessel_Voyage_Standard'].nunique()
                            st.metric("Vessel Names Standardized", 
                                    f"{std_vessels} unique",
                                    delta=f"Reduced from {orig_vessels}")
                    
                    with col2:
                        if 'Bond or Non Bond' in processed_df.columns:
                            bond_pct = processed_df['Bond or Non Bond'].value_counts(normalize=True).get('BOND', 0)
                            st.metric("Bond Shipments", f"{bond_pct:.1%}")
                    
                    with col3:
                        if 'LCL,FCL Status' in processed_df.columns:
                            valid_status = processed_df['LCL,FCL Status'].notna().sum()
                            st.metric("Valid Status Values", f"{valid_status}/{len(processed_df)}")

                    # PDF-specific metrics if uploaded
                    if uploaded_pdf:
                        st.markdown("---")
                        st.markdown("### 📄 PDF Integration Results")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if 'PDF_VESSEL_MATCH' in processed_df.columns:
                                matched = processed_df['PDF_VESSEL_MATCH'].notna().sum()
                                st.metric("Vessels Matched with PDF", 
                                        f"{matched}/{len(processed_df)}",
                                        help="Number of vessels successfully matched with operation meeting minutes")
                        
                        with col2:
                            if 'FORECAST ETA' in processed_df.columns:
                                updated_etas = processed_df['FORECAST ETA'].notna().sum()
                                st.metric("Updated ETAs from PDF", 
                                        f"{updated_etas}",
                                        help="ETAs updated from meeting minutes forecast")
                        


                        # Manual Timeline Adjustment section
                        if 'PDF_VESSEL_MATCH' in processed_df.columns:
                            st.markdown("---")
                            st.markdown("### ✏️ Manual Timeline Adjustment")
                            
                            # Get unique matched vessels with all needed columns
                            matched_vessels = processed_df[
                                processed_df['PDF_VESSEL_MATCH'].notna()
                            ]['Vessel_Voyage_Standard'].unique()
                            
                            if len(matched_vessels) > 0:
                                # Include ALL needed columns including PDF data
                                # Define target column names we expect
                                expected_columns = [
                                    'Vessel_Voyage_Standard', 'ETA', 'ATA', 'ETB', 'Estimated Clearance',
                                    'PDF_VESSEL_MATCH', 'BOOKING ETA', 'FORECAST ETA', 'CURRENT_STATUS'
                                ]

                                # Build a mapping from expected name to actual column (based on similarity)
                                column_mapping = {}
                                actual_columns = list(processed_df.columns)

                                for expected in expected_columns:
                                    # Get the closest match with a cutoff for similarity
                                    matches = difflib.get_close_matches(expected, actual_columns, n=1, cutoff=0.7)
                                    if matches:
                                        column_mapping[expected] = matches[0]

                                # DEBUG: Show mapping results
                                st.markdown("#### 🧪 Column Mapping (Fuzzy Matched)")
                                st.json(column_mapping)

                                # Use only columns that were matched
                                present_editable_cols = list(column_mapping.values())

                                # DEBUG: Warn if anything couldn't be matched
                                unmatched = [col for col in expected_columns if col not in column_mapping]
                                if unmatched:
                                    st.warning(f"⚠️ Could not find close matches for: {', '.join(unmatched)}")
                                else:
                                    st.success("✅ All expected columns were matched using fuzzy logic")

                                # Filter vessel rows using fuzzy-mapped column names
                                vessel_df = processed_df.loc[
                                    processed_df['Vessel_Voyage_Standard'].isin(processed_df[column_mapping['Vessel_Voyage_Standard']]),
                                    present_editable_cols
                                ].copy()


                                
                                # Convert dates to strings for editing
                                for col in ['ETA', 'ATA', 'ETB', 'Estimated Clearance', 'BOOKING ETA', 'FORECAST ETA']:
                                    if col in vessel_df.columns:
                                        vessel_df[col] = vessel_df[col].apply(
                                            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) and pd.api.types.is_datetime64_any_dtype(x) else x
                                        )
                                
                                with st.form("eta_update_form"):
                                    st.write("**Edit vessel timelines (YYYY-MM-DD format):**")
                                    
                                    # Create editor with ALL columns
                                    edited_df = st.data_editor(
                                        vessel_df.sort_values('Vessel_Voyage_Standard'),
                                        column_config={
                                            "ETA": st.column_config.DateColumn(
                                                "ETA (Editable)",
                                                format="YYYY-MM-DD",
                                                help="Estimated Time of Arrival",
                                            ),
                                            "ATA": st.column_config.DateColumn(
                                                "ATA (Auto-updated)",
                                                format="YYYY-MM-DD",
                                                disabled=True,
                                            ),
                                            "ETB": st.column_config.DateColumn(
                                                "ETB (Auto-updated)",
                                                format="YYYY-MM-DD",
                                                disabled=True,
                                            ),
                                            "Estimated Clearance": st.column_config.DateColumn(
                                                "Est. Clearance (Auto-updated)",
                                                format="YYYY-MM-DD",
                                                disabled=True,
                                            ),
                                            "PDF_VESSEL_MATCH": st.column_config.TextColumn(
                                                "Matched PDF Vessel",
                                                disabled=True,
                                            ),
                                            "BOOKING ETA": st.column_config.DateColumn(
                                                "Original Booking ETA",
                                                format="YYYY-MM-DD",
                                                disabled=True,
                                            ),
                                            "FORECAST ETA": st.column_config.DateColumn(
                                                "PDF Forecast ETA",
                                                format="YYYY-MM-DD",
                                                disabled=True,
                                            ),
                                            "CURRENT_STATUS": st.column_config.TextColumn(
                                                "PDF Status",
                                                disabled=True,
                                            )
                                        },
                                        num_rows="fixed",
                                        use_container_width=True,
                                        key="vessel_editor"
                                    )
                                    
                                    # Handle real-time updates
                                    if 'vessel_editor' in st.session_state:
                                        changes = st.session_state.vessel_editor['edited_rows']
                                        if changes:
                                            for idx, change in changes.items():
                                                if 'ETA' in change:
                                                    try:
                                                        new_eta = pd.to_datetime(change['ETA'])
                                                        # Update dependent dates directly in the dataframe
                                                        edited_df.loc[int(idx), 'ATA'] = (new_eta + timedelta(days=1)).strftime('%Y-%m-%d')
                                                        edited_df.loc[int(idx), 'ETB'] = (new_eta + timedelta(days=2)).strftime('%Y-%m-%d')
                                                        edited_df.loc[int(idx), 'Estimated Clearance'] = (new_eta + timedelta(days=3)).strftime('%Y-%m-%d')
                                                    except Exception as e:
                                                        st.error(f"Invalid date format: {e}")
                                    
                                    submitted = st.form_submit_button("💾 Save All Updates")
                                    
                                    if submitted:
                                        # Convert back to datetime and update main dataframe
                                        for col in ['ETA', 'ATA', 'ETB', 'Estimated Clearance']:
                                            if col in edited_df.columns:
                                                edited_df[col] = pd.to_datetime(edited_df[col], errors='coerce')
                                        
                                        # Update the main dataframe
                                        for _, row in edited_df.iterrows():
                                            mask = (
                                                (processed_df['Vessel_Voyage_Standard'] == row['Vessel_Voyage_Standard']) & 
                                                (processed_df['PDF_VESSEL_MATCH'].notna())
                                            )
                                            processed_df.loc[mask, 'ETA'] = row['ETA']
                                            processed_df.loc[mask, 'ATA'] = row['ETA'] + timedelta(days=1) if pd.notna(row['ETA']) else pd.NaT
                                            processed_df.loc[mask, 'ETB'] = row['ETA'] + timedelta(days=2) if pd.notna(row['ETA']) else pd.NaT
                                            processed_df.loc[mask, 'Estimated Clearance'] = row['ETA'] + timedelta(days=3) if pd.notna(row['ETA']) else pd.NaT
                                        
                                        st.success("Vessel timelines updated successfully!")
                                        st.rerun()
                            else:
                                st.info("No matched vessels found for timeline adjustment")


                    # Processing details
                    with st.expander("🔧 Processing Details"):
                        st.markdown("""
                        **Applied Transformations:**
                        - **Vessel Standardization**: Combined vessel+voyage, cleaned special chars, fuzzy-matched similar names
                        - **Bond Classification**: Auto-classified based on origin port and carrier
                        - **Status Cleaning**: Normalized LCL/FCL status values
                        - **Timeline Calculation**: Estimated ATA and clearance dates
                        """)
                        
                        if uploaded_pdf:
                            st.markdown("""
                            **PDF Enhancements:**
                            - Vessel schedule matching with operation meeting minutes
                            - ETA updates from official forecasts
                            - Status cross-validation
                            """)
                        
                        if 'Vessel_Voyage_Standard' in processed_df.columns:
                            st.write("**Example Vessel Standardizations:**")
                            sample_cols = ['Origin Vessel', 'Vessel_Voyage_Standard']
                            if 'PDF_VESSEL_MATCH' in processed_df.columns:
                                sample_cols.append('PDF_VESSEL_MATCH')
                            sample_vessels = processed_df[sample_cols].dropna().drop_duplicates().head(5)
                            st.dataframe(sample_vessels, hide_index=True)

                    # Show processed data
                    st.markdown("---")
                    st.markdown("### 📦 Processed Data Preview")
                    st.dataframe(processed_df.head(10), use_container_width=True)
                    
                    # Download option
                    csv = processed_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Processed Data",
                        data=csv,
                        file_name="dsr_merchant_data_processed.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No data available for processing. Please load data in the analysis tab first.")




def show_summary_statistics(df, title):
    """Helper function to show summary statistics for any DataFrame"""
    st.write(f"**{title} Summary Statistics**")
    
    if df.empty:
        st.warning("No data available for summary")
        return
    
    # Basic stats
    cols = st.columns(3)
    cols[0].metric("Total Records", len(df))
    cols[1].metric("Columns", len(df.columns))
    
    # Show numeric columns stats
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        st.write("**Numeric Columns Statistics**")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    # Show date columns stats if any
    date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    if len(date_cols) > 0:
        st.write("**Date Ranges**")
        date_stats = []
        for col in date_cols:
            min_date = df[col].min()
            max_date = df[col].max()
            date_stats.append({
                "Column": col,
                "Earliest": min_date if pd.notna(min_date) else "N/A",
                "Latest": max_date if pd.notna(max_date) else "N/A"
            })
        st.dataframe(pd.DataFrame(date_stats), use_container_width=True)
    
    # Show value counts for categorical columns
    cat_cols = [col for col in df.columns if df[col].nunique() < 20 and df[col].nunique() > 1]
    if len(cat_cols) > 0:
        st.write("**Category Distributions**")
        for col in cat_cols:
            st.write(f"**{col}**")
            st.bar_chart(df[col].value_counts())


def show_sidebar():
    with st.sidebar:
        #st.image("https://via.placeholder.com/150x50?text=DSR+Processor", width=150)  # Add your logo here
        
        # Configuration Section
        config_expander = st.expander("⚙️ CONFIGURATION", expanded=False)
        with config_expander:
            if st.button("🛠️ Open Settings Panel"):
                st.session_state.show_config = not st.session_state.get('show_config', False)
            
            if st.session_state.get('show_config', False):
                show_configuration_ui()
        
        st.markdown("---")
        st.subheader("🛠️ Processing Parameters")
        
        # Add date filtering toggle
        st.session_state.use_date_filter = st.checkbox(
            "Enable Date Filtering",
            value=False,
            help="Filter records by date range when enabled"
        )
        
        # Only show date selection if filtering is enabled
        if st.session_state.use_date_filter:
            weeks = config_manager.config["global"]["date_range_weeks"]
            selected_date = st.date_input(
                "📅 Reference Date for Shipments",
                value=st.session_state.selected_reference_date,
                help=f"Will show shipments within ±{weeks} weeks of this date"
            )
            st.session_state.selected_reference_date = pd.Timestamp(selected_date)
        
        # Fuzzy Threshold with visual indicator
        threshold = config_manager.config["global"]["fuzzy_threshold"]
        st.progress(threshold/100, text=f"🔍 Fuzzy Matching Threshold: {threshold}%")
        
        st.markdown("---")
        st.subheader("📋 Processing Pipeline")
        
        # Enhanced progress tracker
        steps = [
            (1, "1. Expo Data", st.session_state.expo_processed),
            (2, "2. Maersk Data", st.session_state.maersk_processed),
            (3, "3. Globe Data", st.session_state.globe_processed),
            (4, "4. Scanwell Data", st.session_state.scanwell_processed),
            (5, "5. Final Results", all([
                st.session_state.expo_processed,
                st.session_state.maersk_processed,
                st.session_state.globe_processed,
                st.session_state.scanwell_processed
            ]))
        ]
        
        for step_num, step_name, is_complete in steps:
            if st.session_state.current_step == step_num:
                st.markdown(f"➡️ **{step_name}** (Current Step)")
            elif is_complete:
                st.markdown(f"✅ ~~{step_name}~~ (Completed)")
            else:
                st.markdown(f"◻️ {step_name}")
        
        # Navigation controls with better icons
        if st.session_state.current_step > 1 and st.session_state.current_step < 5:
            if st.button("⏮️ Previous Step"):
                st.session_state.current_step -= 1
                st.rerun()
        
        # Final step actions
        if st.session_state.current_step == 5:
            st.markdown("---")
            st.subheader("📤 Export Results")
            combined = pd.concat([
                st.session_state.expo_data,
                st.session_state.maersk_data,
                st.session_state.globe_data,
                st.session_state.scanwell_data
            ], ignore_index=True)
            
            csv = combined.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Complete Dataset",
                data=csv,
                file_name='dsr_combined_results.csv',
                mime='text/csv',
                help="Download all processed data as a single CSV file"
            )
            
            if st.button("🔄 Start New Processing Run", type="primary"):
                init_session_state()
                st.rerun()



def show_admin_panel():
    st.title("🔐 User Management Panel")
    st.markdown("Manage users, roles, and access levels.")

    # --- User List Table ---
    users = auth.list_users()
    df = pd.DataFrame(users)
    st.markdown("### 👥 Current Users")
    st.dataframe(df, use_container_width=True, height=300)

    st.markdown("---")

    # --- Add User Section ---
    with st.expander("➕ Add New User"):
        st.subheader("Create a New Account")
        with st.form("add_user_form"):
            new_user = st.text_input("Username", placeholder="e.g. johndoe")
            new_pass = st.text_input("Password", type="password")
            new_role = st.selectbox("Role", ["full", "limited", "view_only"])
            submitted = st.form_submit_button("Create User")
            
            if submitted:
                if not new_user or not new_pass:
                    st.warning("Username and password are required.")
                elif auth.add_user(new_user.strip(), new_pass, new_role):
                    st.success(f"✅ User '{new_user}' created.")
                    st.rerun()
                else:
                    st.error("❌ Username already exists.")

    st.markdown("---")

    # --- User Actions Section ---
    st.subheader("⚙️ Manage Existing Users")
    selected_user = st.selectbox("Select User", [u["username"] for u in users])

    # Fetch current user info
    current_info = next(u for u in users if u["username"] == selected_user)
    is_self = selected_user == st.session_state.current_user

    col1, col2 = st.columns(2)

    # --- Change Role ---
    with col1:
        st.markdown("**Change Role**")
        new_role = st.selectbox(
            f"New role for {selected_user}", 
            ["full", "limited", "view_only"], 
            index=["full", "limited", "view_only"].index(current_info["role"]),
            key=f"role_{selected_user}"
        )
        if st.button("Update Role"):
            if auth.update_user_role(selected_user, new_role):
                st.success("Role updated.")
                st.rerun()
            else:
                st.error("Failed to update role.")

    # --- Toggle Active Status ---
    with col2:
        st.markdown("**User Access**")
        current_status = current_info["is_active"]
        toggle_label = "Disable" if current_status else "Enable"

        if is_self:
            st.info("⚠️ You cannot disable your own account.")
        else:
            if st.button(f"{toggle_label} User"):
                if auth.toggle_user_active(selected_user):
                    st.success(f"{selected_user} has been {'disabled' if current_status else 'enabled'}.")
                    st.rerun()
                else:
                    st.error("Failed to toggle status.")

    # --- Reset Password (Optional) ---
    with st.expander("🔁 Reset User Password"):
        st.markdown("Reset the password for a user.")
        reset_user = st.selectbox("Select User", [u["username"] for u in users], key="reset_pw_user")
        new_pw = st.text_input("New Password", type="password", key="new_pw_input")
        if st.button("Reset Password"):
            if auth.update_user_password(reset_user, new_pw):
                st.success(f"Password for '{reset_user}' updated.")
            else:
                st.error("Failed to reset password.")

    # --- Delete User (Admin Only) ---
    st.markdown("### 🗑️ Delete User")

    if is_self:
        st.info("You cannot delete your own account.")
    else:
        with st.expander(f"🗑️ Delete '{selected_user}'"):
            st.warning("This action is irreversible.")
            confirm = st.checkbox("Yes, I want to delete this user.")
            if st.button("Delete User"):
                if confirm:
                    if auth.delete_user(selected_user):
                        st.success(f"User '{selected_user}' has been deleted.")
                        st.rerun()
                    else:
                        st.error("Failed to delete user.")
                else:
                    st.error("Please confirm before deleting.")



def apply_dark_mode(dark_mode_enabled):
    """Apply dark mode CSS styles if enabled"""
    if dark_mode_enabled:
        dark_css = """
        <style>
            /* Dark mode styles */
            .stApp {
                background-color: #0E1117;
                color: #FAFAFA;
            }
            [data-testid="stSidebar"] {
                background-color: #0E1117 !important;
            }
            .stMarkdown, .stText, h1, h2, h3, h4, h5, h6 {
                color: #FAFAFA !important;
            }
            .dataframe {
                background-color: #0E1117 !important;
                color: #FAFAFA !important;
            }
            [data-testid="stExpander"] {
                background: rgba(30, 30, 30, 0.8) !important;
                border-color: #444 !important;
            }
            .stButton>button {
                transition: all 0.3s ease;
                background-color: #4F8BF9;
                color: white !important;
            }
            .stButton>button:hover {
                transform: scale(1.02);
                background-color: #3a7de9;
            }
            .stProgress>div>div>div {
                background-color: #4CAF50;
            }
        </style>
        """
        st.markdown(dark_css, unsafe_allow_html=True)
    else:
        st.markdown("""
        <style>
            .stButton>button {
                transition: all 0.3s ease;
            }
            .stButton>button:hover {
                transform: scale(1.02);
            }
            .stProgress>div>div>div {
                background-color: #4CAF50;
            }
            [data-testid="stExpander"] {
                background: rgba(245, 245, 245, 0.1);
                border-radius: 8px;
            }
        </style>
        """, unsafe_allow_html=True)


def main():
    print("1. Starting main() execution")  # Debug point 1
    
    # Page configuration
    st.set_page_config(
        page_title="DSR Processor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    print("2. Page config set")  # Debug point 2

    # Initialize dark mode state
    if 'dark_mode' not in st.session_state:
        st.session_state.dark_mode = False
        print("3. Initialized dark_mode in session_state")  # Debug point 3
    
    # Apply appropriate CSS based on mode
    print(f"4. Applying dark mode (current state: {st.session_state.dark_mode})")  # Debug point 4
    apply_dark_mode(st.session_state.dark_mode)

    # Authentication flow
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        print("5. Initialized authenticated in session_state")  # Debug point 5
    
    if 'current_user' not in st.session_state:
        st.session_state.current_user = None
        print("6. Initialized current_user in session_state")  # Debug point 6
    
    if 'access_level' not in st.session_state:
        st.session_state.access_level = None
        print("7. Initialized access_level in session_state")  # Debug point 7
    
    # Show login page if not authenticated
    if not st.session_state.authenticated:
        print("8. User not authenticated, showing login page")  # Debug point 8
        show_login_page()
        return
    
    # For authenticated users
    print("9. User is authenticated, routing to appropriate app")  # Debug point 9
    if st.session_state.authenticated:
        print(f"10. User access level: {st.session_state.access_level}")  # Debug point 10
        if st.session_state.access_level == "full":  # Admin
            print("11. Routing to admin app")  # Debug point 11
            run_admin_app()
        elif st.session_state.access_level == "limited":  # Legato
            print("12. Routing to legato app")  # Debug point 12
            run_legato_app()
        elif st.session_state.access_level == "view_only":  # Business
            print("13. Routing to business app")  # Debug point 13
            run_business_app()

    # Configuration manager setup
    if 'config_manager' not in st.session_state:
        print("14. Initializing config_manager")  # Debug point 14
        st.session_state.config_manager = ConfigurationManager()
    
    # Show sidebar and current step
    print("15. Showing sidebar")  # Debug point 15
    #show_sidebar()
    print("16. Showing current step")  # Debug point 16
    #show_current_step()
    
    # Configuration validation
    if st.session_state.get('show_config', False):
        print("17. Validating configuration")  # Debug point 17
        errors = st.session_state.config_manager.validate_config()
        if errors:
            print(f"18. Found {len(errors)} config errors")  # Debug point 18
            st.toast("⚠️ Configuration errors detected!", icon="⚠️")
            with st.expander("Configuration Issues", expanded=True):
                for error in errors:
                    st.error(error)
    
    print("19. Main() execution complete")  # Debug point 19



def admin_required(func):
    def wrapper(*args, **kwargs):
        if st.session_state.get('access_level') != "full":
            st.warning("Admin access required")
            return
        return func(*args, **kwargs)
    return wrapper

def show_login_page():
    """Displays the login page without calling set_page_config()"""
    # Custom CSS for styling
    st.markdown("""
    <style>
        .login-container {
            max-width: 600px;
            margin: 0 auto;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        }
        .login-title {
            text-align: center;
            color: #2c3e50;
            margin-bottom: 2rem;
        }
        .login-form {
            padding: 2rem;
            background: white;
            border-radius: 8px;
        }
        .stButton>button {
            width: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 0.5rem;
            border-radius: 4px;
            font-size: 1rem;
        }
        .stTextInput>div>div>input {
            padding: 0.5rem;
            border-radius: 4px;
        }
    </style>
    """, unsafe_allow_html=True)

    # Login container
    with st.container():
        st.markdown('<div class="login-container">', unsafe_allow_html=True)
        
        # Title with logo
        st.markdown("""
        <div class="login-title">
            <h1>📊 Piyadasa-DSR Data Processor</h1>
            <p>Streamlined shipment data analysis platform</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Login form
        with st.form("login_form"):
            st.markdown('<div class="login-form">', unsafe_allow_html=True)
            
            username = st.text_input("👤 Username", placeholder="Enter your username")
            password = st.text_input("🔒 Password", type="password", placeholder="Enter your password")
            
            login_button = st.form_submit_button("Login")
            
            if login_button:
                if not username or not password:
                    st.error("Please enter both username and password")
                else:
                    with st.spinner("Authenticating..."):
                        result = auth.authenticate(username, password)
                        if result["authenticated"]:
                            st.session_state.authenticated = True
                            st.session_state.current_user = username
                            st.session_state.access_level = result["access_level"]
                            st.success("Login successful!")
                            time.sleep(1)  # Let the user see the success message
                            st.rerun()
                        else:
                            st.error("Invalid credentials. Please try again.")
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Footer
        st.markdown("""
        <div style="text-align: center; margin-top: 2rem; color: #7f8c8d;">
            <p>Need help? Contact IT support</p>
            <p>v2.0.0</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)


def clean_air_data(df):
    df_clean = df.copy()
    
    # Step 1: Remove blank HAWB rows
    hawb_col = df_clean.columns[8]
    df_clean = df_clean[~df_clean[hawb_col].isna()]
    df_clean = df_clean[df_clean[hawb_col].astype(str).str.strip() != '']

    # Step 2: Fix known misalignment based on markers
    start_marker = "MF2415972"
    end_marker = "H24504871-P8;"
    start_idx = df_clean[df_clean.iloc[:, 0].astype(str).str.strip() == start_marker].index.min()
    end_idx = df_clean[df_clean.iloc[:, 0].astype(str).str.strip() == end_marker].index.max()

    if pd.notna(start_idx) and pd.notna(end_idx):
        block_df = df_clean.loc[start_idx:end_idx].copy()
        exclude_values = {start_marker, end_marker}
        misaligned_mask = ~block_df.iloc[:, 0].astype(str).str.strip().isin(exclude_values)
        misaligned_indices = block_df[misaligned_mask].index

        cols = df_clean.columns.tolist()
        df_clean.loc[misaligned_indices, cols[:-1]] = df_clean.loc[misaligned_indices, cols[1:]].values
        df_clean.loc[misaligned_indices, cols[-1]] = pd.NA

    # Step 3: Drop duplicates
    df_clean = df_clean.drop_duplicates()

    # Step 4: Date cleaning
    def clean_and_parse_date(date_str):
        if pd.isna(date_str) or not isinstance(date_str, str):
            return pd.NaT
        date_str = re.sub(r"[^\d./]", "", date_str.strip())

        if re.fullmatch(r"\d{1,4}", date_str):
            return pd.NaT

        match_short_year = re.match(r"(\d{1,2})[./](\d{1,2})[./](\d{2})$", date_str)
        if match_short_year:
            dd, mm, yy = match_short_year.groups()
            date_str = f"{dd}.{mm}.20{yy}"

        parts = re.split(r"[./]", date_str)
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            a, b, c = map(int, parts)
            if c < 100:
                c += 2000
            if b > 12:
                dd, mm, yyyy = b, a, c
            else:
                dd, mm, yyyy = a, b, c
            try:
                return datetime(yyyy, mm, dd)
            except:
                return pd.NaT

        for fmt in ("%d.%m.%Y", "%d/%m/%Y"):
            try:
                return pd.to_datetime(date_str, format=fmt, errors="coerce")
            except:
                continue

        return pd.NaT

    eta_col = df_clean.columns[10]
    clearance_col = df_clean.columns[11]
    df_clean['ETA_PARSED'] = df_clean[eta_col].apply(clean_and_parse_date)
    df_clean['CLEARANCE_PARSED'] = df_clean[clearance_col].apply(clean_and_parse_date)

    return df_clean

def run_admin_app():
    # Initialize dark mode state
    if 'dark_mode' not in st.session_state:
        st.session_state.dark_mode = False
    
    # Sidebar - Keep essential processing controls
    with st.sidebar:
        # Dark mode toggle
        dark_mode = st.toggle("🌙 Dark Mode", 
                            value=st.session_state.dark_mode,
                            key="admin_dark_mode_toggle")
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
        
        # Logout button
        if st.button("🚪 Logout", key="admin_logout_btn"):
            st.session_state.authenticated = False
            st.session_state.current_user = None
            st.session_state.access_level = None
            st.rerun()

        # Processing Parameters (keep this in sidebar)
        st.markdown("---")
        st.subheader("🛠️ Processing Parameters")
        
        # Date filtering toggle
        st.session_state.use_date_filter = st.checkbox(
            "Enable Date Filtering",
            value=st.session_state.get('use_date_filter', False),
            help="Filter records by date range when enabled"
        )
        
        if st.session_state.use_date_filter:
            weeks = config_manager.config["global"]["date_range_weeks"]
            selected_date = st.date_input(
                "📅 Reference Date",
                value=st.session_state.get('selected_reference_date', datetime.now().date()),
                help=f"±{weeks} weeks range"
            )
            st.session_state.selected_reference_date = pd.Timestamp(selected_date)
        
        # Processing Pipeline Status
        st.markdown("---")
        st.subheader("📋 Pipeline Status")
        steps = [
            (1, "1. Expo Data", st.session_state.get('expo_processed', False)),
            (2, "2. Maersk Data", st.session_state.get('maersk_processed', False)),
            (3, "3. Globe Data", st.session_state.get('globe_processed', False)),
            (4, "4. Scanwell Data", st.session_state.get('scanwell_processed', False))
        ]
        
        for step_num, step_name, is_complete in steps:
            if st.session_state.get('current_step', 1) == step_num:
                st.markdown(f"➡️ **{step_name}**")
            elif is_complete:
                st.markdown(f"✅ ~~{step_name}~~")
            else:
                st.markdown(f"◻️ {step_name}")
        
        # Final step download and reset
        if st.session_state.get('current_step', 1) == 5:
            st.markdown("---")
            combined = pd.concat([
                st.session_state.expo_data,
                st.session_state.maersk_data,
                st.session_state.globe_data,
                st.session_state.scanwell_data
            ], ignore_index=True)
            
            csv = combined.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Results",
                data=csv,
                file_name='dsr_combined_results.csv',
                mime='text/csv'
            )
            
            if st.button("🔄 Reset Pipeline", type="primary"):
                init_session_state()
                st.rerun()

    # Main Content Area with new Configuration tab
    sea_tab, air_tab, config_tab = st.tabs(["🌊 Sea Shipments", "✈️ Air Shipments", "⚙️ Configuration"])
    
    with sea_tab:
        # Your existing sea shipment processing code
        show_current_step()

    with air_tab:
        st.header("✈️ Air Shipment Management")

        # Ensure air_data is initialized
        if 'air_data' not in st.session_state:
            st.session_state.air_data = pd.DataFrame()

        # Column layout for controls
        col1, col2 = st.columns(2)

        with col1:
            air_source = st.radio(
                "Data Source",
                options=["Google Sheets", "Upload File"],
                key="air_source_radio"
            )

        with col2:
            if air_source == "Google Sheets":
                if st.button("🔄 Fetch Air Data", key="fetch_air_data"):
                    try:
                        response = requests.get(
                            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRuIk9kF1gp-TlmhHpPkcC75IeWFm4r_QTYtJOePHa0ZcoEaVvUmJiHL6i0R2282YjKfuBwu7-B1CNt/pub?output=xlsx",
                            verify=False
                        )
                        st.session_state.air_data = pd.read_excel(BytesIO(response.content), sheet_name="AIR")
                        st.success("Air data loaded successfully!")
                    except Exception as e:
                        st.error(f"Failed to fetch air data: {str(e)}")
            else:
                air_file = st.file_uploader(
                    "Upload Air Shipment File", 
                    type=['xlsx', 'csv'],
                    key="air_file_uploader"
                )
                if air_file:
                    if air_file.name.endswith('.xlsx'):
                        try:
                            st.session_state.air_data = pd.read_excel(air_file, sheet_name="Previous")
                            st.success("Air data uploaded from 'Previous' sheet successfully!")
                        except ValueError:
                            st.error("Sheet named 'Previous' not found in the uploaded Excel file.")
                    else:
                        st.session_state.air_data = pd.read_csv(air_file)
                        st.success("Air data CSV uploaded successfully!")

        # Separate display section
        if not st.session_state.air_data.empty:
            st.markdown("---")
            st.subheader("📦 Loaded Air Shipment Data")
            st.dataframe(st.session_state.air_data, use_container_width=True)




    with config_tab:
            st.header("⚙️ System Configuration")
            
            # Use your existing configuration UI function but remove sidebar references
            def show_config_tab_ui():
                with st.expander("⚙️ Settings", expanded=True):  # Changed from sidebar.expander
                    tab1, tab2, tab3, tab4 = st.tabs(["Columns", "Mappings", "Global", "Data Sources"])
                    
                    with tab1:
                        show_column_management()
                        
                    with tab2:
                        show_mapping_management()
                        
                    with tab3:
                        show_global_settings()

                    with tab4:
                        show_data_sources_management()
            
            # User management section
            show_admin_panel()
            
            # Configuration section
            st.markdown("---")
            st.subheader("🛠️ System Settings")
            show_config_tab_ui()  # Using the modified version of your config UI
            


def run_legato_app():
    st.title("📊 Legato Team - Consolidated Shipment Overview")
    
    # Initialize sidebar as closed by default
    st.markdown("""
    <style>
        [data-testid="collapsedControl"] {
            display: none
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Add logout button to sidebar
    with st.sidebar:
        if st.button("🚪 Logout", key="legato_logout_btn"):
            st.session_state.authenticated = False
            st.session_state.current_user = None
            st.session_state.access_level = None
            st.rerun()
        
        # Add dark mode toggle
        if 'dark_mode' not in st.session_state:
            st.session_state.dark_mode = False
        dark_mode = st.toggle("🌙 Dark Mode", 
                            value=st.session_state.dark_mode,
                            key="legato_dark_mode_toggle")
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
    
    # Initialize combined data
    if 'combined_data' not in st.session_state:
        st.session_state.combined_data = pd.DataFrame()
    
    # Data source options
    analysis_option = st.radio(
        "Choose data source:",
        options=["Fetch from Google Sheets", "Upload new file"],
        index=0,
        horizontal=True,
        key="data_source"
    )
    
    if analysis_option == "Fetch from Google Sheets":
        try:
            import requests
            from io import BytesIO
            import urllib3
            
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            google_sheets_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRuIk9kF1gp-TlmhHpPkcC75IeWFm4r_QTYtJOePHa0ZcoEaVvUmJiHL6i0R2282YjKfuBwu7-B1CNt/pub?output=xlsx'
            
            if st.button("🔄 Fetch Data", key="fetch_data_btn"):
                with st.spinner("Fetching data from Google Sheets..."):
                    response = requests.get(google_sheets_url, verify=False)
                    
                    # Read both sheets
                    sea_data = pd.read_excel(BytesIO(response.content), sheet_name="SEA")
                    air_data = pd.read_excel(BytesIO(response.content), sheet_name="AIR")
                    
                    # Standardize columns and add shipment type
                    sea_data['Shipment Type'] = 'Sea'
                    air_data['Shipment Type'] = 'Air'
                    
                    # Create common column mapping
                    sea_columns = {
                        'HBL': 'Tracking Number',
                        'Inv #': 'Invoice Number',
                        'PO #': 'PO Number',
                        'CUSDEC No': 'Customs Declaration',
                        'CUSDEC Date': 'Customs Date',
                        'ETA': 'Arrival Date',
                        'Shipper': 'Shipper',
                        'Origin': 'Origin Country',
                        'Port': 'Destination Port',
                        'Description of Goods': 'Description',
                        'Gross Weight': 'Weight',
                        'No of Cartons': 'Cartons'
                    }
                    
                    air_columns = {
                        'HAWB': 'Tracking Number',
                        'INVOICE #': 'Invoice Number',
                        'PO #': 'PO Number',
                        'CUSDEC NO': 'Customs Declaration',
                        'CLEARANCE_PARSED': 'Customs Date',
                        'ETA_PARSED': 'Arrival Date',
                        'SHIPPER': 'Shipper',
                        'COUNTRY OF ORIGIN': 'Origin Country',
                        'CHARGEABLE WEIGHT': 'Weight',
                        'CTNS': 'Cartons'
                    }
                    
                    # Select and rename columns for sea data
                    available_sea_cols = [col for col in sea_columns.keys() if col in sea_data.columns]
                    sea_data = sea_data[available_sea_cols + ['Shipment Type']]
                    sea_data = sea_data.rename(columns=sea_columns)
                    
                    # Select and rename columns for air data
                    available_air_cols = [col for col in air_columns.keys() if col in air_data.columns]
                    air_data = air_data[available_air_cols + ['Shipment Type']]
                    air_data = air_data.rename(columns=air_columns)
                    
                    # Combine the data
                    st.session_state.combined_data = pd.concat([sea_data, air_data], ignore_index=True)
                    st.success("✅ Data fetched and consolidated successfully!")
        
        except Exception as e:
            st.error(f"Failed to fetch data: {str(e)}")
    
    else:  # Upload new file
        uploaded_file = st.file_uploader(
            "Upload shipment file (Excel with SEA and AIR sheets)",
            type=['xlsx'],
            key='file_uploader'
        )
        
        if uploaded_file:
            try:
                with st.spinner("Processing uploaded file..."):
                    # Read both sheets
                    sea_data = pd.read_excel(uploaded_file, sheet_name="SEA")
                    air_data = pd.read_excel(uploaded_file, sheet_name="AIR")
                    
                    # Standardize columns and add shipment type
                    sea_data['Shipment Type'] = 'Sea'
                    air_data['Shipment Type'] = 'Air'
                    
                    # Create common column mapping (same as above)
                    sea_columns = {
                        'HBL': 'Tracking Number',
                        'Inv #': 'Invoice Number',
                        'PO #': 'PO Number',
                        'CUSDEC No': 'Customs Declaration',
                        'CUSDEC Date': 'Customs Date',
                        'ETA': 'Arrival Date',
                        'Shipper': 'Shipper',
                        'Origin': 'Origin Country',
                        'Port': 'Destination Port',
                        'Description of Goods': 'Description',
                        'Gross Weight': 'Weight',
                        'No of Cartons': 'Cartons'
                    }
                    
                    air_columns = {
                        'HAWB': 'Tracking Number',
                        'INVOICE #': 'Invoice Number',
                        'PO #': 'PO Number',
                        'CUSDEC NO': 'Customs Declaration',
                        'CLEARANCE_PARSED': 'Customs Date',
                        'ETA_PARSED': 'Arrival Date',
                        'SHIPPER': 'Shipper',
                        'COUNTRY OF ORIGIN': 'Origin Country',
                        'CHARGEABLE WEIGHT': 'Weight',
                        'CTNS': 'Cartons'
                    }
                    
                    # Select and rename columns for sea data
                    available_sea_cols = [col for col in sea_columns.keys() if col in sea_data.columns]
                    sea_data = sea_data[available_sea_cols + ['Shipment Type']]
                    sea_data = sea_data.rename(columns=sea_columns)
                    
                    # Select and rename columns for air data
                    available_air_cols = [col for col in air_columns.keys() if col in air_data.columns]
                    air_data = air_data[available_air_cols + ['Shipment Type']]
                    air_data = air_data.rename(columns=air_columns)
                    
                    # Combine the data
                    st.session_state.combined_data = pd.concat([sea_data, air_data], ignore_index=True)
                    st.success("✅ Data uploaded and consolidated successfully!")
            
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    
    # Display and filter consolidated data
    if not st.session_state.combined_data.empty:
        st.success(f"✨ Consolidated data loaded! Total records: {len(st.session_state.combined_data):,}")
        
        # Key metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Shipments", len(st.session_state.combined_data))
        with col2:
            st.metric("Sea Shipments", len(st.session_state.combined_data[st.session_state.combined_data['Shipment Type'] == 'Sea']))
        with col3:
            st.metric("Air Shipments", len(st.session_state.combined_data[st.session_state.combined_data['Shipment Type'] == 'Air']))
        
        # Filters
        st.markdown("### 🔎 Filter Shipments")
        
        # Create filter columns
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            # Shipment type filter
            shipment_types = st.multiselect(
                "Shipment Type",
                options=st.session_state.combined_data['Shipment Type'].unique(),
                default=st.session_state.combined_data['Shipment Type'].unique(),
                key="shipment_type_filter"
            )
            
            # Tracking number filter
            tracking_numbers = st.multiselect(
                "Tracking Number (HBL/HAWB)",
                options=st.session_state.combined_data['Tracking Number'].dropna().unique(),
                key="tracking_number_filter"
            )
        
        with filter_col2:
            # Invoice filter
            invoice_numbers = st.multiselect(
                "Invoice Number",
                options=st.session_state.combined_data['Invoice Number'].dropna().unique(),
                key="invoice_filter"
            )
            
            # PO filter
            po_numbers = st.multiselect(
                "PO Number",
                options=st.session_state.combined_data['PO Number'].dropna().unique(),
                key="po_filter"
            )
        
        with filter_col3:
            # Customs declaration filter
            customs_declarations = st.multiselect(
                "Customs Declaration",
                options=st.session_state.combined_data['Customs Declaration'].dropna().unique(),
                key="customs_filter"
            )
            
            # Shipper filter
            shippers = st.multiselect(
                "Shipper",
                options=st.session_state.combined_data['Shipper'].dropna().unique(),
                key="shipper_filter"
            )
        
        # Apply filters
        filtered_data = st.session_state.combined_data.copy()
        
        if shipment_types:
            filtered_data = filtered_data[filtered_data['Shipment Type'].isin(shipment_types)]
        if tracking_numbers:
            filtered_data = filtered_data[filtered_data['Tracking Number'].isin(tracking_numbers)]
        if invoice_numbers:
            filtered_data = filtered_data[filtered_data['Invoice Number'].isin(invoice_numbers)]
        if po_numbers:
            filtered_data = filtered_data[filtered_data['PO Number'].isin(po_numbers)]
        if customs_declarations:
            filtered_data = filtered_data[filtered_data['Customs Declaration'].isin(customs_declarations)]
        if shippers:
            filtered_data = filtered_data[filtered_data['Shipper'].isin(shippers)]
        
        st.info(f"🔍 Showing {len(filtered_data):,} filtered records")
        
        # Display filtered data
        with st.expander("🔍 View Filtered Data", expanded=True):
            # Select columns to display (in desired order)
            display_columns = [
                'Shipment Type',
                'Tracking Number',
                'Invoice Number',
                'PO Number',
                'Shipper',
                'Origin Country',
                'Arrival Date',
                'Customs Declaration',
                'Customs Date',
                'Weight',
                'Cartons',
                'Description'
            ]
            
            # Ensure columns exist before selecting
            available_cols = [col for col in display_columns if col in filtered_data.columns]
            display_df = filtered_data[available_cols]
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400
            )
        
        # Download buttons
        st.markdown("---")
        csv = filtered_data.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="💾 Download Filtered Data",
            data=csv,
            file_name='legato_consolidated_shipments.csv',
            mime='text/csv',
            key="consolidated_data_download"
        )


## China
@st.cache_data(ttl=1800)
def fetch_bookings_bytes(region="china"):
    url = config_manager.config["data_sources"][region]["bookings_url"]
    response = requests.get(url, verify=False)
    return response.content

def load_bookings_data(region="china"):
    content = fetch_bookings_bytes(region)
    xls = pd.ExcelFile(BytesIO(content))
    
    # Get default sheet from config
    if region == "china":
        default_sheet = config_manager.config["data_sources"][region]["sheets"]["bookings"]["default_sheet"]
    else:
        default_sheet = config_manager.config["data_sources"][region]["default_sheet"]
    
    # Verify sheet exists
    if default_sheet not in xls.sheet_names:
        st.warning(f"Default sheet '{default_sheet}' not found. Using first sheet.")
        default_sheet = xls.sheet_names[0]
    
    return xls, default_sheet




@st.cache_data(ttl=1800)
def fetch_delivery_status_bytes(region="china"):
    url = config_manager.config["data_sources"][region]["delivery_url"]    
    response = requests.get(url, verify=False)
    return response.content

def load_delivery_status_data(region="china"):
    content = fetch_delivery_status_bytes(region)
    xls = pd.ExcelFile(BytesIO(content))  # Only this part is repeated
    return xls

@st.cache_data
def load_sheet_data_cached(_xls, sheet_name):
    return pd.read_excel(_xls, sheet_name=sheet_name)




def booking_tracker_horizontal_clean(df, delivery_df):
    if df.empty:
        st.markdown("### 🚚 Booking Status Tracker (Loading...)")
        st.markdown("<div style='height: 100px; background: #f0f0f0; border-radius: 8px;'></div>", unsafe_allow_html=True)
        return

    row = df.iloc[0]
    so_number = str(row.get("S/O", "")).strip()
    hawb_key = f"VLL{so_number}"

    # Booking Received
    booking_date = pd.to_datetime(row.get("Booking received date", None), errors='coerce')
    booking_display = booking_date.strftime('%Y-%m-%d') if pd.notna(booking_date) else "—"

    # Booking Confirmed
    etd = row.get("ETD", None)
    eta = row.get("ETA updated", None)
    confirmed_active = pd.notna(etd) or pd.notna(eta)

    # Flight Assigned
    remarks = str(row.get("REMARKS", ""))
    has_mawb = bool(re.search(r'\d{3}-\d{8}', remarks))
    etd_date = pd.to_datetime(etd, errors='coerce')
    etd_display = f"ETD: {etd_date.strftime('%Y-%m-%d')}" if pd.notna(etd_date) else "ETD: —"

    # Delivered Stage: Load and match delivery sheet
    try:
        delivery_xls = pd.ExcelFile(BytesIO(fetch_delivery_status_bytes()))
        #st.session_state.delivery_air_df = pd.read_excel(delivery_xls, sheet_name="AIR")
        #delivery_xls = load_delivery_status_data("china")
        air_sheet = config_manager.config["data_sources"]["china"]["sheets"]["delivery"]["air_sheet"]
        delivery_df = pd.read_excel(delivery_xls, sheet_name=air_sheet)
        delivery_match = delivery_df[delivery_df["HAWB"].astype(str).str.contains(hawb_key, case=False, na=False)]

        delivered_active = not delivery_match.empty
        clearance_date = delivery_match["CLEARANCE DEAT"].iloc[0] if delivered_active else "—"
        consignee = delivery_match["CONSIGNEE"].iloc[0] if delivered_active else "—"
        vehicle_no = delivery_match["VEHIECAL NO"].iloc[0] if delivered_active else "—"

        clearance_display = f"Clearance: {clearance_date}" if pd.notna(clearance_date) else "Clearance: —"
        extra_details = f"Consignee: {consignee}<br>Vehicle No: {vehicle_no}" if delivered_active else ""

    except Exception as e:
        st.warning(f"Could not load delivery status: {e}")
        delivered_active = False
        clearance_display = "Clearance: —"
        extra_details = ""

    # Tracker stages
    stages = [
        {"icon": "🟢", "name": "Booking Received", "date": booking_display},
        {"icon": "🟢" if confirmed_active else "⚪", "name": "Booking Confirmed", "date": "—"},
        {"icon": "🟢" if has_mawb else "⚪", "name": "Flight Assigned", "date": etd_display},
        {
            "icon": "🟢" if delivered_active else "⚪",
            "name": "Delivered",
            "date": clearance_display,
            "details": extra_details
        },
    ]

    # Render horizontal tracker
    tracker_html = "<div style='display: flex; justify-content: space-around; align-items: flex-start;'>"
    for i, stage in enumerate(stages):
        tracker_html += f"""
        <div style='text-align: center; font-size: 16px;'>
            <div style='font-size: 24px;'>{stage['icon']}</div>
            <div><strong>{stage['name']}</strong></div>
            <div style='font-size: 12px; color: gray;'>{stage['date']}</div>
        """
        if "details" in stage and stage["details"]:
            tracker_html += f"<div style='font-size: 11px; color: #444;'>{stage['details']}</div>"
        tracker_html += "</div>"

        if i < len(stages) - 1:
            tracker_html += "<div style='font-size: 24px; margin: 0 10px;'>➡️</div>"

    tracker_html += "</div>"

    st.markdown("### 🚚 Booking Status Tracker")
    st.markdown(
    "<div style='margin-bottom: 10px; font-size: 14px;'>"
    "🟢 <span style='color: green;'>Completed</span> &nbsp;&nbsp;&nbsp; "
    "⚪ <span style='color: gray;'>Pending</span>"
    "</div>",
    unsafe_allow_html=True
    )
    st.markdown(tracker_html, unsafe_allow_html=True)


# India
# Function to fetch and load India booking data
@st.cache_data(ttl=1800)
def fetch_india_bookings_bytes():
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQh9PCBs5Z-aq1QV_gBuXYlkv2bjm0-12An8yqVpzntQNctS-vdis7oVnaJ-2BA8J3sCp8U9vYMd2Nh/pub?output=xlsx"
    response = requests.get(url, verify=False)
    return response.content


def load_india_bookings_data():
    content = fetch_india_bookings_bytes()
    xls = pd.ExcelFile(BytesIO(content))
    return xls


def india_booking_tracker(booking_row):
    if booking_row.empty:
        st.info("No booking data available for tracker.")
        return

    # Bookings Received
    received_date = pd.to_datetime(booking_row.get("Booking received date", None), errors='coerce')
    received_active = pd.notna(received_date)
    received_display = received_date.strftime('%Y-%m-%d') if received_active else "—"

    # Booking Confirmed
    confirmed_active = pd.notna(pd.to_datetime(booking_row.get("Approval Received Date", None), errors='coerce'))

    # Flight Assigned
    etd_date = pd.to_datetime(booking_row.get("ETD", None), errors='coerce')
    etd_active = pd.notna(etd_date)
    etd_display = etd_date.strftime('%Y-%m-%d') if etd_active else "—"

    # Delivered Stage
    hawb_col = next((col for col in booking_row.index if "hawb" in col.lower()), None)
    hawb_no = str(booking_row.get(hawb_col, "")).strip() if hawb_col else ""

    delivered_active = False
    delivered_display = "—"

    if hawb_no:
        delivery_df = st.session_state.get("delivery_air_df", pd.DataFrame())
        delivery_col = next((col for col in delivery_df.columns if "hawb" in col.lower()), None)

        if delivery_col:
            delivery_match = delivery_df[delivery_df[delivery_col].astype(str).str.strip() == hawb_no]

            if not delivery_match.empty:
                delivered_date = pd.to_datetime(delivery_match.iloc[0].get("Delivered Date", None), errors='coerce')
                delivered_active = pd.notna(delivered_date)
                if delivered_active:
                    delivered_display = delivered_date.strftime('%Y-%m-%d')

    # Tracker stages
    stages = [
        {"icon": "🟢" if received_active else "⚪", "name": "Bookings Received", "date": received_display},
        {"icon": "🟢" if confirmed_active else "⚪", "name": "Booking Confirmed", "date": None},
        {"icon": "🟢" if etd_active else "⚪", "name": "Flight Assigned", "date": etd_display},
        {"icon": "🟢" if delivered_active else "⚪", "name": "Delivered", "date": delivered_display},
    ]

    # Render tracker
    tracker_html = "<div style='display: flex; justify-content: space-around; align-items: flex-start;'>"
    for i, stage in enumerate(stages):
        tracker_html += f"""
        <div style='text-align: center; font-size: 16px;'>
            <div style='font-size: 24px;'>{stage['icon']}</div>
            <div><strong>{stage['name']}</strong></div>
        """
        if stage["date"]:
            tracker_html += f"<div style='font-size: 12px; color: gray;'>{stage['date']}</div>"
        tracker_html += "</div>"

        if i < len(stages) - 1:
            tracker_html += "<div style='font-size: 24px; margin: 0 10px;'>➡️</div>"

    tracker_html += "</div>"

    st.markdown("### 📦 Booking Status Tracker")
    st.markdown(
        "<div style='margin-bottom: 10px; font-size: 14px;'>"
        "🟢 <span style='color: green;'>Completed</span> &nbsp;&nbsp;&nbsp; "
        "⚪ <span style='color: gray;'>Pending</span>"
        "</div>",
        unsafe_allow_html=True
    )
    st.markdown(tracker_html, unsafe_allow_html=True)



def run_business_app():
    # Initialize loading states
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    
    # Add refresh button at top left
    col1, col2 = st.columns([1, 10])
    with col1:
        if st.button("🔄 Refresh", key="business_refresh_button"):
            # Clear relevant caches and session state
            st.cache_data.clear()
            st.session_state.clear()
            st.session_state.data_loaded = False
            st.rerun()
    
    with col2:
        st.title("📊 Business Team - Shipment Overview")    
    # Show loading placeholder if data not loaded yet
    if not st.session_state.data_loaded:
        with st.container():
            st.markdown("""
            <div style="text-align: center; padding: 50px;">
                <h2>📊 Loading Business Dashboard</h2>
                <p>Please wait while we load your shipment data...</p>
                <div class="spinner"></div>
            </div>
            <style>
                .spinner {
                    border: 5px solid #f3f3f3;
                    border-top: 5px solid #3498db;
                    border-radius: 50%;
                    width: 50px;
                    height: 50px;
                    animation: spin 1s linear infinite;
                    margin: 20px auto;
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            </style>
            """, unsafe_allow_html=True)
        
        # Load data in the background
        try:
            # Initialize data
            if "delivery_air_df" not in st.session_state:
                delivery_xls = pd.ExcelFile(BytesIO(fetch_delivery_status_bytes()))
                st.session_state.delivery_air_df = pd.read_excel(delivery_xls, sheet_name="AIR")
            
            st.session_state.data_loaded = True
            st.rerun()  # Refresh to show the actual content
            
        except Exception as e:
            st.error(f"Failed to load data: {str(e)}")
        return

    
    # Initialize sidebar as closed by default
    st.markdown("""
    <style>
        [data-testid="collapsedControl"] {
            display: none
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Add logout button to sidebar
    with st.sidebar:
        if st.button("🚪 Logout", key="business_logout_btn"):
            st.session_state.authenticated = False
            st.session_state.current_user = None
            st.session_state.access_level = None
            st.rerun()
        
        # Add dark mode toggle
        if 'dark_mode' not in st.session_state:
            st.session_state.dark_mode = False
        dark_mode = st.toggle("🌙 Dark Mode", 
                            value=st.session_state.dark_mode,
                            key="business_dark_mode_toggle")
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
    
    # Create tabs for shipment types
    bookings_tab, sea_tab, air_tab = st.tabs(["📅 Current Bookings Air","🌊 Sea Delivery Status", "✈️ Air Delivered Status"])


    with bookings_tab:
        with st.container(border=True):
            china_tab, india_tab, other_tab = st.tabs(["China/HK", "India", "Other Regions"])

            with china_tab:

                try:
                    xls,_ = load_bookings_data("china")

                    all_sheets = xls.sheet_names
                    month_sheets = []
                    for sheet in all_sheets:
                        if any(month.lower() in sheet.lower() for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                                                                            'jul', 'aug', 'sep', 'oct', 'nov', 'dec']):
                            month_sheets.append(sheet)
                        elif re.search(r'\d{1,2}[-/]\d{4}', sheet):
                            month_sheets.append(sheet)

                    if not month_sheets:
                        month_sheets = all_sheets
                        st.warning("No month/year patterns detected in sheet names - showing all sheets")

                    def sort_sheets_by_date(sheet_list):
                        dated_sheets = []
                        for sheet in sheet_list:
                            try:
                                dt = pd.to_datetime(sheet, errors='coerce')
                                if pd.notna(dt):
                                    dated_sheets.append((dt, sheet))
                            except:
                                continue
                        dated_sheets.sort(reverse=True, key=lambda x: x[0])
                        return [sheet for (dt, sheet) in dated_sheets]

                    sorted_sheets = sort_sheets_by_date(month_sheets)
                    if not sorted_sheets:
                        sorted_sheets = month_sheets[::-1]

                    col1, col2 = st.columns([2, 3])

                    with col1:
                        selected_sheet = st.selectbox(
                            "Select Month Sheet",
                            options=sorted_sheets,
                            index=0,
                            key="china_sheet_select"
                        )

                    # Load sheet only once per session or when sheet changes
                    if 'china_last_sheet' not in st.session_state or st.session_state.china_last_sheet != selected_sheet:
                        df = load_sheet_data_cached(xls, selected_sheet)
                        st.session_state.china_booking_df = df
                        st.session_state.china_last_sheet = selected_sheet
                        st.session_state.current_booking = pd.DataFrame()

                    df = st.session_state.china_booking_df 

                    with col2:
                        filter_mode = st.radio("Filter by", options=["S/O", "PO #"], horizontal=True, key="filter_mode_choice")

                        if filter_mode == "S/O":
                            # Clear selected_so since we’re not using it in this mode
                            st.session_state.selected_so = None

                            so_input = st.text_input("Enter S/O Number:", key="china_so_input")

                            if so_input:
                                booking = df[df['S/O'].astype(str).str.contains(so_input, case=False, na=False)]
                                if not booking.empty:
                                    st.session_state.current_booking = booking
                                else:
                                    st.warning("No booking found with that S/O number.")
                                    st.session_state.current_booking = pd.DataFrame()  # Clear if nothing found


                        else:  # Filter by PO #
                            po_input = st.text_input("Enter PO Number:", key="china_po_input")

                            if po_input:
                                matching_po_rows = df[df['PO #'].astype(str).str.contains(po_input, case=False, na=False)]

                                if not matching_po_rows.empty:
                                    so_options = matching_po_rows['S/O'].dropna().unique().tolist()

                                    selected_so = st.selectbox("Select an S/O for tracker:", options=so_options, key="china_select_so")

                                    # Assign all records for the PO, but also highlight the selected S/O in tracker
                                    st.session_state.current_booking = matching_po_rows
                                    st.session_state.selected_so = selected_so

                                else:
                                    st.warning("No bookings found with that PO number.")


                    # Display the booking if found
                    if 'current_booking' in st.session_state and not st.session_state.current_booking.empty:
                        st.subheader("Booking Details")

                        df_to_show = st.session_state.current_booking

                        # If a PO was used and an S/O was selected, optionally highlight that S/O
                        if 'selected_so' in st.session_state and st.session_state.selected_so and 'S/O' in df_to_show.columns:
                            df_to_show = df_to_show.copy()
                            df_to_show.insert(0, 'Selected S/O', df_to_show['S/O'] == st.session_state.selected_so)

                        display_columns = [
                            'S/O', 'Booking received date', 'Origin Air Port', 'Consignee',
                            'Shipper', 'PO #', 'Merchant', 'Terms', 'Booked Date (MM/DD/YYYY)',
                            'Product type', 'Booking Quantity', 'Package Type',
                            'Booking Gross KGS', 'Volume kgs', 'Booked CBM (m3)', 'Cargo Ready Date',
                            'Shipment Type'
                        ]
                        available_cols = [col for col in display_columns if col in df_to_show.columns]
                        st.dataframe(
                            df_to_show[available_cols],
                            use_container_width=True,
                            hide_index=True
                        )

                        # Only run tracker on selected S/O if PO mode used
                        if 'selected_so' in st.session_state and st.session_state.selected_so and 'S/O' in df_to_show.columns:
                            tracker_df = df_to_show[df_to_show['S/O'] == st.session_state.selected_so]
                            if not tracker_df.empty:
                                booking_tracker_horizontal_clean(tracker_df, st.session_state.delivery_air_df)
                            else:
                                st.warning("No tracker data found for the selected S/O.")
                        else:
                            if not df_to_show.empty:
                                booking_tracker_horizontal_clean(df_to_show, st.session_state.delivery_air_df)
                            else:
                                st.warning("No booking data to display.")

                    else:
                        st.info("Enter details to view booking details and tracker")

                except Exception as e:
                    st.error(f"Failed to load bookings data: {str(e)}")

            with india_tab:
                try:
                    xls = load_india_bookings_data()
                    sheet_names = xls.sheet_names

                    default_sheet = "APRIL 25-26"
                    if default_sheet in sheet_names:
                        default_index = sheet_names.index(default_sheet)
                    else:
                        default_index = 0

                    # --- Layout: Sheet and Filter mode side-by-side ---
                    col1, col2 = st.columns(2)
                    with col1:
                        selected_sheet = st.selectbox("Select Sheet", options=sheet_names, index=default_index, key="india_sheet_select")
                    with col2:
                        filter_mode = st.radio("Filter by", options=["SNO", "PO #"], horizontal=True, key="india_filter_mode")

                    # Load selected sheet (cache)
                    if "india_last_sheet" not in st.session_state or st.session_state.india_last_sheet != selected_sheet:
                        df = load_sheet_data_cached(xls, selected_sheet)
                        st.session_state.india_booking_df = df
                        st.session_state.india_last_sheet = selected_sheet
                        st.session_state.india_current_booking = pd.DataFrame()
                        st.session_state.india_selected_sno = None

                    df = st.session_state.india_booking_df

                    if filter_mode == "SNO":
                        st.session_state.india_selected_sno = None
                        sno_input = st.text_input("Enter SNO to search (Exact Match):", key="india_sno_input")

                        if sno_input:
                            first_col = df.columns[0]
                            booking = df[df[first_col].astype(str).str.strip() == sno_input.strip()]
                            if not booking.empty:
                                st.session_state.india_current_booking = booking
                            else:
                                st.warning("No booking found with that exact SNO.")
                                st.session_state.india_current_booking = pd.DataFrame()

                    else:  # PO # mode
                        po_input = st.text_input("Enter PO Number:", key="india_po_input")

                        if po_input:
                            if 'PO #' not in df.columns or 'SNO' not in df.columns:
                                st.warning("Required columns 'PO #' or 'SNO' not found in the sheet.")
                            else:
                                matching_rows = df[df['PO #'].astype(str).str.contains(po_input.strip(), case=False, na=False)]
                                if not matching_rows.empty:
                                    sno_options = matching_rows['SNO'].dropna().unique().tolist()

                                    selected_sno = st.selectbox("Select SNO for details:", options=sno_options, key="india_po_sno_select")

                                    # Assign all rows for the PO, but highlight selected SNO
                                    st.session_state.india_current_booking = matching_rows
                                    st.session_state.india_selected_sno = selected_sno
                                else:
                                    st.warning("No bookings found with that PO number.")

                    # Show result
                    if not st.session_state.india_current_booking.empty:
                        st.subheader("India Booking Details")

                        df_to_show = st.session_state.india_current_booking

                        # Optional: highlight selected SNO
                        if st.session_state.india_selected_sno and 'SNO' in df_to_show.columns:
                            df_to_show = df_to_show.copy()
                            df_to_show.insert(0, 'Selected SNO', df_to_show['SNO'] == st.session_state.india_selected_sno)

                        display_columns = [
                            "SNO", "Booking received date", "Origin Air Port", "Consignee", "Shipper",
                            "Invoice #", "PO #", "Merchant", "Terms", "Booked Date (MM/DD/YYYY)",
                            "Product type", "Booking Quantity", "Package Type", "Booking KGS",
                            "Booked CBM", "Cargo Ready Date"
                        ]
                        available_cols = [col for col in display_columns if col in df_to_show.columns]
                        st.dataframe(df_to_show[available_cols], use_container_width=True, hide_index=True)

                        # Run tracker for selected SNO if available
                        tracker_df = df_to_show
                        if st.session_state.india_selected_sno:
                            tracker_df = df_to_show[df_to_show['SNO'] == st.session_state.india_selected_sno]

                        if not tracker_df.empty:
                            india_booking_tracker(tracker_df.iloc[0])
                        else:
                            st.warning("No tracker data found for selected SNO.")

                    else:
                        st.info("Enter SNO or PO # to view booking details.")

                except Exception as e:
                    st.error(f"Failed to load India booking data: {str(e)}")


    with sea_tab:
        with st.container(border=True):
            # Initialize session state for sea data
            if 'sea_data' not in st.session_state:
                st.session_state.sea_data = pd.DataFrame()
                st.session_state.sea_data_loaded = False

            # Data source options
            analysis_option = st.radio(
                "Choose data source:",
                options=["Fetch from Google Sheets", "Use previously processed data", "Upload new file"],
                index=0,
                horizontal=True,
                key="business_sea_data_source"
            )

            # Load data based on selection
            if not st.session_state.sea_data_loaded:
                with st.spinner("Loading sea data..."):
                    if analysis_option == "Fetch from Google Sheets":
                        try:
                            @st.cache_data(ttl=3600)  # Cache for 1 hour
                            def fetch_sea_data():
                                url = config_manager.config["data_sources"]["sea"]["url"]
                                response = requests.get(url, verify=False)
                                sea_sheet=config_manager.config["data_sources"]["sea"]["sheet_name"]
                                return pd.read_excel(BytesIO(response.content), sheet_name=sea_sheet)
                            
                            st.session_state.sea_data = fetch_sea_data()
                            st.session_state.sea_data_loaded = True
                            st.success("✅ Sea data fetched successfully!")
                        except Exception as e:
                            st.error(f"Failed to fetch sea data: {str(e)}")

                    elif analysis_option == "Upload new file":
                        sea_file = st.file_uploader(
                            "Upload sea shipment file",
                            type=['xlsx', 'csv'],
                            key='business_sea_file_uploader'
                        )
                        if sea_file:
                            try:
                                if sea_file.name.endswith('.csv'):
                                    st.session_state.sea_data = pd.read_csv(sea_file)
                                else:
                                    st.session_state.sea_data = pd.read_excel(sea_file, sheet_name="SEA")
                                st.session_state.sea_data_loaded = True
                            except Exception as e:
                                st.error(f"Error reading file: {str(e)}")
                    else:
                        if (st.session_state.expo_processed or st.session_state.maersk_processed or 
                            st.session_state.globe_processed or st.session_state.scanwell_processed):
                            st.session_state.sea_data = pd.concat([
                                st.session_state.expo_data,
                                st.session_state.maersk_data,
                                st.session_state.globe_data,
                                st.session_state.scanwell_data
                            ], ignore_index=True)
                            st.session_state.sea_data_loaded = True
                        else:
                            st.warning("No processed sea data available")

            # Display and filter data if loaded
            if not st.session_state.sea_data.empty:
                # Create a copy for filtering to avoid modifying original
                sea_df = st.session_state.sea_data.copy()
                
                # Convert ETA to datetime once
                if 'ETA' in sea_df.columns:
                    sea_df['ETA'] = pd.to_datetime(sea_df['ETA'], errors='coerce')

                # Display metrics
                st.success(f"✨ Sea data loaded! Records: {len(sea_df):,}")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Unique HBLs", sea_df['HBL'].nunique())
                    if 'PO #' in sea_df.columns:
                        st.metric("Unique POs", sea_df['PO #'].nunique())
                with col2:
                    if 'Gross Weight' in sea_df.columns:
                        total_weight = sea_df['Gross Weight'].sum()
                        st.metric("Total Gross Weight", f"{total_weight:,.2f} kg")

                # Filters
                st.markdown("### 🔎 Filter Sea Records")
                with st.expander("Filter Options", expanded=False):
                    filter_col1, filter_col2 = st.columns(2)
                    
                    with filter_col1:
                        hbl_options = sea_df['HBL'].dropna().unique()
                        selected_hbls = st.multiselect("Filter by HBL", options=hbl_options)
                        
                        if 'PO #' in sea_df.columns:
                            po_options = sea_df['PO #'].dropna().unique()
                            selected_pos = st.multiselect("Filter by PO #", options=po_options)

                    with filter_col2:
                        if 'ETA' in sea_df.columns:
                            eta_options = sea_df['ETA'].dt.date.dropna().unique()
                            selected_eta_dates = st.multiselect("Filter by ETA Date", options=eta_options)
                        
                        if 'Origin Vessel' in sea_df.columns:
                            vessel_options = sea_df['Origin Vessel'].dropna().unique()
                            selected_vessels = st.multiselect("Filter by Vessel", options=vessel_options)

                # Apply filters
                if selected_hbls:
                    sea_df = sea_df[sea_df['HBL'].isin(selected_hbls)]
                if 'selected_pos' in locals() and selected_pos:
                    sea_df = sea_df[sea_df['PO #'].isin(selected_pos)]
                if 'selected_eta_dates' in locals() and selected_eta_dates:
                    sea_df = sea_df[sea_df['ETA'].dt.date.isin(selected_eta_dates)]
                if 'selected_vessels' in locals() and selected_vessels:
                    sea_df = sea_df[sea_df['Origin Vessel'].isin(selected_vessels)]

                st.info(f"🔍 Showing {len(sea_df):,} filtered sea records")

               # Delivery Status Legend
                st.markdown("""
                <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 10px;">
                    <div style="width: 20px; height: 20px; background-color: lightgreen; border: 1px solid #ccc;"></div>
                    <span>Delivered</span>
                    <div style="width: 20px; height: 20px; background-color: transparent; border: 1px solid #ccc; margin-left: 10px;"></div>
                    <span>Pending</span>
                </div>
                """, unsafe_allow_html=True)
                filtered_sea=sea_df.copy()
                # Show filtered data with delivery status
                if 'Delivery date' in filtered_sea.columns:
                    try:
                        # Create display dataframe
                        sea_display_columns = {
                            "HBL": "HBL",
                            "PO #": "PO #",
                            "ETA": "ETA",
                            "Origin": "Origin",
                            "Port": "Port",
                            "Shipper": "Shipper",
                            "Consignee": "Consignee",
                            "Inv #": "Inv #",
                            "Description": "Description",
                            "Delivery date": "Delivery date"
                        }
                        
                        available_cols = [col for col in sea_display_columns.keys() if col in filtered_sea.columns]
                        renamed_cols = {col: sea_display_columns[col] for col in available_cols}
                        display_sea_df = filtered_sea.drop(columns=["Sheet"], errors="ignore")

                        # Apply color formatting
                        def color_delivery_status(row):
                            if pd.notna(row['Delivery date']):
                                return ['background-color: lightgreen'] * len(row)
                            return [''] * len(row)
                        
                        with st.expander("🔍 View Filtered Sea Data", expanded=True):
                            st.dataframe(
                                display_sea_df.style.apply(color_delivery_status, axis=1),
                                use_container_width=True,
                                height=400
                            )
                        
                        # Delivery status metrics
                        st.markdown("### 🚚 Delivery Status Overview")
                        delivery_counts = {
                            'Delivered': filtered_sea['Delivery date'].notna().sum(),
                            'Pending': len(filtered_sea) - filtered_sea['Delivery date'].notna().sum()
                        }
                        
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.dataframe(
                                pd.DataFrame.from_dict(delivery_counts, orient='index', columns=['Count']),
                                width=200
                            )
                        
                        with col2:
                            fig = px.pie(
                                values=list(delivery_counts.values()),
                                names=list(delivery_counts.keys()),
                                color=list(delivery_counts.keys()),
                                color_discrete_map={'Delivered':'lightgreen','Pending':'lightgray'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                    except Exception as e:
                        st.warning(f"Couldn't process delivery status: {e}")
                        with st.expander("🔍 View Filtered Data", expanded=True):
                            st.dataframe(
                                filtered_sea,
                                use_container_width=True,
                                height=400
                            )
                else:
                    with st.expander("🔍 View Filtered Data", expanded=True):
                        st.dataframe(
                            filtered_sea,
                            use_container_width=True,
                            height=400
                        )
                
                # Download buttons
                st.markdown("---")
                csv = filtered_sea.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="💾 Download Filtered Sea Data",
                    data=csv,
                    file_name='business_filtered_sea_data.csv',
                    mime='text/csv',
                    key="business_sea_data_download"
                )


    with air_tab:
        with st.container(border=True):
            # Initialize air data - use session state if available
            if 'delivery_air_df' in st.session_state:
                st.session_state.air_data = st.session_state.delivery_air_df
            else:
                st.session_state.air_data = pd.DataFrame()
                
            
            # Air shipment display and filtering
            if not st.session_state.air_data.empty:
                st.success(f"✈️ Air data loaded! Records: {len(st.session_state.air_data):,}")
                
                # Air shipment metrics
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Unique HAWBs", st.session_state.air_data['HAWB'].nunique())

                with col2:
                    if 'PO #' in st.session_state.air_data.columns:
                        st.metric("Unique POs", st.session_state.air_data['PO #'].nunique())

                # Air shipment filters
                st.markdown("### 🔎 Filter Air Records")
                filter_col1, filter_col2 = st.columns(2)

                with filter_col1:
                    selected_hawbs = st.multiselect(
                        "Filter by HAWB",
                        options=st.session_state.air_data['HAWB'].dropna().unique(),
                        key="business_air_hawb_filter"
                    )
                    
                    if 'PO #' in st.session_state.air_data.columns:
                        selected_pos = st.multiselect(
                            "Filter by PO #",
                            options=st.session_state.air_data['PO #'].dropna().unique(),
                            key="business_air_po_filter"
                        )

                with filter_col2:
                    if 'INVOICE #' in st.session_state.air_data.columns:
                        selected_invs = st.multiselect(
                            "Filter by Invoice #",
                            options=st.session_state.air_data['INVOICE #'].dropna().unique(),
                            key="business_air_inv_filter"
                        )
                    
                    if 'SHIPPER' in st.session_state.air_data.columns:
                        selected_shippers = st.multiselect(
                            "Filter by Shipper",
                            options=st.session_state.air_data['SHIPPER'].dropna().unique(),
                            key="business_air_shipper_filter"
                        )

                # Apply filters
                filtered_air = st.session_state.air_data.copy()
                if selected_hawbs:
                    filtered_air = filtered_air[filtered_air['HAWB'].isin(selected_hawbs)]
                if 'PO #' in st.session_state.air_data.columns and selected_pos:
                    filtered_air = filtered_air[filtered_air['PO #'].isin(selected_pos)]
                if 'INVOICE #' in st.session_state.air_data.columns and selected_invs:
                    filtered_air = filtered_air[filtered_air['INVOICE #'].isin(selected_invs)]
                if 'SHIPPER' in st.session_state.air_data.columns and selected_shippers:
                    filtered_air = filtered_air[filtered_air['SHIPPER'].isin(selected_shippers)]

                st.info(f"🔍 Showing {len(filtered_air):,} filtered air records")

                # Show filtered air data with delivery status
                try:
                    # Select and rename columns for air shipment display
                    air_display_columns = {
                        "INVOICE #": "Invoice #",
                        "PO #": "PO #",
                        "SHIPPER": "Shipper",
                        "CONSIGNEE": "Consignee",
                        "INCOTERM": "Incoterm",
                        "COUNTRY OF ORIGIN": "Country of Origin",
                        "CTNS": "CTNS",
                        "CHARGEABLE WEIGHT": "Chargeable Weight",
                        "HAWB": "HAWB",
                        "MAWB": "MAWB",
                        "CUSDEC NO": "Cusdec No",
                        "VEHIECAL NO": "Vehicle No",
                        "ETA DATE ": "ETA DATE",
                        "CLEARANCE DEAT": "Clearance Date"
                    }

                    # Ensure only available columns are included in the display
                    available_cols = [col for col in air_display_columns.keys() if col in filtered_air.columns]
                    renamed_cols = {col: air_display_columns[col] for col in available_cols}
                    
                    # Prepare the data for display
                    display_air_df = filtered_air[available_cols].rename(columns=renamed_cols)

                    with st.expander("🔍 View Filtered Air Data", expanded=True):
                        st.dataframe(
                            display_air_df,
                            use_container_width=True,
                            height=400
                        )

                except Exception as e:
                    st.warning(f"Couldn't process air shipment data: {e}")
                    with st.expander("🔍 View Filtered Data", expanded=True):
                        st.dataframe(
                            filtered_air,
                            use_container_width=True,
                            height=400
                        )

                # Download buttons
                st.markdown("---")
                csv = filtered_air.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="💾 Download Filtered Air Data",
                    data=csv,
                    file_name='business_filtered_air_data.csv',
                    mime='text/csv',
                    key="business_air_data_download"
                )
            else:
                st.warning("No air delivery data found. Please check if China delivery data was loaded properly.")




if __name__ == "__main__":
    main()


