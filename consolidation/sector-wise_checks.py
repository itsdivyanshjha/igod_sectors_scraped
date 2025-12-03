"""
Enhanced sector-wise consolidation analysis for IGOD portals.

This script identifies REAL consolidation opportunities by detecting:
1. Similar organizational types across states (e.g., multiple "Agriculture Departments")
2. Similar functional entities (e.g., multiple Police/High Court websites)
3. Patterns within sectors that indicate duplicate functionality

Usage:
    python sector-wise_checks.py [input_excel.xlsx] [output_report.xlsx]

Defaults:
    input_excel.xlsx   = "sector-wise_urls.xlsx"
    output_report.xlsx = "igod_sector_consolidation_working_only.xlsx"

The script expects the input workbook to have:
    - Sheet: "Working Portals"
    - Required columns: "Sector", "Name", "URL"

Outputs a single Excel workbook with:
    - "Summary": Overview of consolidation opportunities by sector
    - "All_Patterns": Cross-sector patterns (Police, Courts, Universities, etc.)
    - One sheet per sector: Detailed consolidation clusters within that sector
"""

import sys
from pathlib import Path
from urllib.parse import urlparse
import re
from collections import defaultdict, Counter

import pandas as pd


########################
# Pattern definitions #
########################

# Common organizational types that appear across states/locations
ORG_TYPE_PATTERNS = {
    "Police": r'\bPolice\b',
    "High Court": r'\bHigh\s+Court\b',
    "District Court": r'\bDistrict\s+Court\b',
    "Municipal Corporation": r'\bMunicipal\s+Corporation\b',
    "Development Authority": r'\bDevelopment\s+Authority\b',
    "University": r'\bUniversity\b',
    "College": r'\bCollege\b',
    "Institute": r'\bInstitute\b',
    "Board": r'\bBoard\b',
    "Department": r'\bDepartment\b',
    "Directorate": r'\bDirectorate\b',
    "Corporation": r'\bCorporation\b',
    "Authority": r'\bAuthority\b',
    "Council": r'\bCouncil\b',
    "Commission": r'\bCommission\b',
    "Tribunal": r'\bTribunal\b',
}

# Indian states and UTs for location extraction
INDIAN_STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
    "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
    "Andaman and Nicobar", "Chandigarh", "Dadra and Nagar Haveli", "Daman and Diu",
    "Delhi", "Jammu and Kashmir", "Ladakh", "Lakshadweep", "Puducherry"
]

# Functional keywords for deeper analysis
FUNCTIONAL_KEYWORDS = {
    "Agriculture": [r'\bAgric', r'\bFarm', r'\bKrishi\b', r'\bRait'],
    "Health": [r'\bHealth\b', r'\bMedical\b', r'\bHospital\b'],
    "Education": [r'\bEducation\b', r'\bSchool\b', r'\bUniversity\b', r'\bCollege\b'],
    "Transport": [r'\bTransport\b', r'\bRoad\b', r'\bHighway\b', r'\bRTO\b'],
    "Water": [r'\bWater\b', r'\bJal\b', r'\bIrrigation\b'],
    "Power": [r'\bPower\b', r'\bElectr', r'\bEnergy\b'],
    "Revenue": [r'\bRevenue\b', r'\bLand\s+Record', r'\bRegistry\b'],
    "Legal": [r'\bCourt\b', r'\bJudicial\b', r'\bLegal\b', r'\bTribunal\b'],
}


########################
# Helper functions    #
########################

def extract_state(title: str) -> str:
    """Extract state/UT name from portal title."""
    if not isinstance(title, str):
        return "Unknown"
    
    title_lower = title.lower()
    for state in INDIAN_STATES:
        if state.lower() in title_lower:
            return state
    
    return "Central/Unknown"


def extract_org_type(title: str) -> str:
    """Extract primary organization type from title."""
    if not isinstance(title, str):
        return None

    # Check in priority order (more specific first)
    priority_order = [
        "High Court", "District Court", "Municipal Corporation", 
        "Development Authority", "Police", "University", "College",
        "Institute", "Tribunal", "Commission", "Directorate", 
        "Department", "Board", "Council", "Authority", "Corporation"
    ]
    
    for org_type in priority_order:
        if re.search(ORG_TYPE_PATTERNS[org_type], title, re.IGNORECASE):
            return org_type
    
        return None


def extract_functional_area(title: str) -> list:
    """Extract functional areas from title (can be multiple)."""
    if not isinstance(title, str):
        return []
    
    areas = []
    for area, patterns in FUNCTIONAL_KEYWORDS.items():
        for pattern in patterns:
            if re.search(pattern, title, re.IGNORECASE):
                areas.append(area)
                break
    
    return areas


def normalize_title(title: str) -> str:
    """
    Normalize title for similarity comparison.
    Remove state names, common articles, extra whitespace.
    """
    if not isinstance(title, str):
        return ""
    
    normalized = title.lower()
    
    # Remove state names
    for state in INDIAN_STATES:
        normalized = re.sub(r'\b' + re.escape(state.lower()) + r'\b', '', normalized)
    
    # Remove common words
    common_words = ['the', 'of', 'and', '&', 'govt', 'government']
    for word in common_words:
        normalized = re.sub(r'\b' + word + r'\b', '', normalized)
    
    # Remove special characters and extra spaces
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def calculate_title_similarity(title1: str, title2: str) -> float:
    """
    Calculate simple word-based similarity between two titles.
    Returns ratio of common words to total unique words.
    """
    words1 = set(normalize_title(title1).split())
    words2 = set(normalize_title(title2).split())
    
    if not words1 or not words2:
        return 0.0
    
    common = words1.intersection(words2)
    total = words1.union(words2)
    
    return len(common) / len(total) if total else 0.0


def make_sheet_name(idx: int, name: str) -> str:
    """Create a safe Excel sheet name (max 31 chars)."""
    base = name.replace("&", "and")
    base = re.sub(r"[^0-9A-Za-z \-]", "", base)
    base = re.sub(r"\s+", "_", base.strip())
    base = base[:25]  # leave room for index
    return f"{idx:02d}_{base}"


##################
# Core functions #
##################

def load_working_df(input_path: Path, sheet_name: str = "Working Portals") -> pd.DataFrame:
    """Read the working portals sheet and add analysis features."""
    xls = pd.ExcelFile(input_path)
    df = pd.read_excel(xls, sheet_name=sheet_name)

    expected = ["Sector", "Name", "URL"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # Rename for consistency
    df = df.rename(columns={"Name": "Title", "URL": "Link"})

    # Filter to only entries with valid URLs
    df = df[df["Link"].notna() & (df["Link"].astype(str).str.strip() != "")].copy()

    # Extract analysis features
    df["State"] = df["Title"].apply(extract_state)
    df["OrgType"] = df["Title"].apply(extract_org_type)
    df["Domain"] = df["Link"].apply(lambda x: urlparse(x).netloc.lower() if isinstance(x, str) else None)

    return df


def find_cross_sector_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find patterns that span across sectors (e.g., all Police portals,
    all High Courts, all Universities regardless of sector).
    """
    patterns = []
    
    for org_type in ORG_TYPE_PATTERNS.keys():
        subset = df[df["OrgType"] == org_type].copy()
        
        if len(subset) >= 3:  # Only patterns with 3+ instances
            # Group by state to show distribution
            state_counts = subset["State"].value_counts()
            
            for idx, row in subset.iterrows():
                patterns.append({
                    "Pattern": org_type,
                    "Total_Instances": len(subset),
                    "Sector": row["Sector"],
                    "State": row["State"],
                    "Title": row["Title"],
                    "Link": row["Link"],
                })
    
    if not patterns:
        return pd.DataFrame()
    
    df_patterns = pd.DataFrame(patterns)
    df_patterns = df_patterns.sort_values(["Pattern", "Sector", "State", "Title"])
    
    return df_patterns


def find_sector_clusters(df_sector: pd.DataFrame, sector_name: str) -> list:
    """
    Find consolidation clusters within a single sector.
    Returns list of cluster dictionaries.
    """
    clusters = []
    
    # Method 1: Group by OrgType + similar normalized titles
    if "OrgType" in df_sector.columns:
        for org_type in df_sector["OrgType"].dropna().unique():
            subset = df_sector[df_sector["OrgType"] == org_type].copy()
            
            if len(subset) >= 2:
                # Check if they have similar base functionality
                titles = subset["Title"].tolist()
                normalized = [normalize_title(t) for t in titles]
                
                # If normalized titles are similar, it's a cluster
                # Simple heuristic: if they share the same org type and 50%+ word overlap
                similarities = []
                for i in range(len(titles)):
                    for j in range(i+1, len(titles)):
                        sim = calculate_title_similarity(titles[i], titles[j])
                        similarities.append(sim)
                
                avg_sim = sum(similarities) / len(similarities) if similarities else 0
                
                if avg_sim >= 0.3 or len(subset) >= 3:  # Either similar or many instances
                    cluster_key = f"{org_type}_cluster"
                    
                    for idx, row in subset.iterrows():
                        clusters.append({
                            "Sector": sector_name,
                            "Cluster": cluster_key,
                            "ClusterType": org_type,
                            "ClusterSize": len(subset),
                            "State": row["State"],
                            "Title": row["Title"],
                            "Link": row["Link"],
                        })
    
    # Method 2: Group by functional area keywords
    for area, patterns in FUNCTIONAL_KEYWORDS.items():
        matching = []
        for idx, row in df_sector.iterrows():
            title = row["Title"]
            if any(re.search(p, title, re.IGNORECASE) for p in patterns):
                matching.append(row)
        
        if len(matching) >= 3:  # 3+ portals in same functional area
            cluster_key = f"{area}_functional"
            
            for row in matching:
                # Check if not already in a cluster
                existing = [c for c in clusters if c["Title"] == row["Title"]]
                if not existing:
                    clusters.append({
                        "Sector": sector_name,
                        "Cluster": cluster_key,
                        "ClusterType": f"{area} (Functional)",
                        "ClusterSize": len(matching),
                        "State": row["State"],
                        "Title": row["Title"],
                        "Link": row["Link"],
                    })
    
    return clusters


def generate_summary(df: pd.DataFrame, sector_clusters: dict, cross_sector_df: pd.DataFrame) -> pd.DataFrame:
    """Generate summary sheet showing consolidation opportunities by sector."""
    sectors = sorted(df["Sector"].dropna().unique())
    
    summary_rows = []
    
    for idx, sector in enumerate(sectors, start=1):
        df_sec = df[df["Sector"] == sector]
        total_portals = len(df_sec)
        
        # Count clusters in this sector
        clusters = sector_clusters.get(sector, [])
        unique_clusters = len(set(c["Cluster"] for c in clusters))
        portals_in_clusters = len(clusters)
        
        # Calculate consolidation potential
        potential_reduction = portals_in_clusters - unique_clusters if unique_clusters > 0 else 0
        
        sheet_name = make_sheet_name(idx, sector) if unique_clusters > 0 else ""
        
        summary_rows.append({
            "Sector": sector,
            "Sheet": sheet_name,
            "Total Portals": total_portals,
            "Consolidation Clusters": unique_clusters,
            "Portals in Clusters": portals_in_clusters,
            "Potential Reduction": potential_reduction,
            "Savings %": f"{(potential_reduction/total_portals*100):.1f}%" if total_portals > 0 else "0%",
        })
    
    df_summary = pd.DataFrame(summary_rows)
    
    return df_summary


############
#   Main   #
############

def main():
    # Handle CLI args
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("sector-wise_urls.xlsx")
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("igod_sector_consolidation.xlsx")

    print(f"Reading working portals from: {input_path}")
    df = load_working_df(input_path)
    
    print(f"Loaded {len(df)} portals across {df['Sector'].nunique()} sectors")
    
    # ===== Step 1: Find cross-sector patterns =====
    print("\nAnalyzing cross-sector patterns...")
    df_patterns = find_cross_sector_patterns(df)
    
    if len(df_patterns) > 0:
        pattern_counts = df_patterns.groupby("Pattern")["Total_Instances"].first().to_dict()
        print(f"  Found {len(pattern_counts)} cross-sector patterns:")
        for pattern, count in sorted(pattern_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"    - {pattern}: {count} instances")
    
    # ===== Step 2: Find sector-specific clusters =====
    print("\nAnalyzing sector-specific consolidation opportunities...")
    sectors = sorted(df["Sector"].dropna().unique())
    sector_clusters = {}
    sector_sheets = {}

    for idx, sector in enumerate(sectors, start=1):
        df_sec = df[df["Sector"] == sector].copy()
        clusters = find_sector_clusters(df_sec, sector)
        
        if clusters:
            sector_clusters[sector] = clusters
            
            # Create sheet
            df_cluster = pd.DataFrame(clusters)
            df_cluster = df_cluster.sort_values(["Cluster", "State", "Title"])
            
            sheet_name = make_sheet_name(idx, sector)
            sector_sheets[sheet_name] = df_cluster
            
            unique_clusters = len(set(c["Cluster"] for c in clusters))
            print(f"  {sector}: {unique_clusters} clusters, {len(clusters)} portals")
    
    # ===== Step 3: Generate summary =====
    print("\nGenerating summary...")
    df_summary = generate_summary(df, sector_clusters, df_patterns)
    
    total_potential = df_summary["Potential Reduction"].sum()
    print(f"  Total potential portal reduction: {total_potential} portals")
    
    # ===== Step 4: Create consolidated "All_Clusters" sheet =====
    print("\nCreating consolidated clusters sheet...")
    all_clusters_data = []
    for sector, clusters in sector_clusters.items():
        all_clusters_data.extend(clusters)
    
    if all_clusters_data:
        df_all_clusters = pd.DataFrame(all_clusters_data)
        df_all_clusters = df_all_clusters.sort_values(["Sector", "Cluster", "State", "Title"])
    else:
        df_all_clusters = pd.DataFrame()

    # ===== Step 5: Write output =====
    print(f"\nWriting report to: {output_path}")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Summary sheet
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

        # Consolidated all clusters sheet (NEW!)
        if len(df_all_clusters) > 0:
            df_all_clusters.to_excel(writer, sheet_name="All_Clusters", index=False)
        
        # Cross-sector patterns sheet
        if len(df_patterns) > 0:
            df_patterns.to_excel(writer, sheet_name="All_Patterns", index=False)
        
        # Sector-specific sheets
        for sheet_name, df_sheet in sector_sheets.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)

    print("\n" + "="*60)
    print("CONSOLIDATION ANALYSIS COMPLETE")
    print("="*60)
    print(f"\n📊 Results:")
    print(f"  • Total portals analyzed: {len(df)}")
    print(f"  • Sectors analyzed: {len(sectors)}")
    print(f"  • Cross-sector patterns: {len(df_patterns) if len(df_patterns) > 0 else 0}")
    print(f"  • Sectors with consolidation opportunities: {len(sector_sheets)}")
    print(f"  • Potential portal reduction: {total_potential} portals")
    print(f"\n📁 Output file: {output_path}")
    print(f"\n📋 Sheets generated:")
    print(f"  • Summary: Sector-wise consolidation overview with savings %")
    print(f"  • All_Clusters: ALL sector clusters in one sheet (easy sharing!)")
    print(f"  • All_Patterns: Cross-sector duplicates (Police, Courts, etc.)")
    print(f"  • {len(sector_sheets)} sector-specific detail sheets")
    print(f"\n💡 Next steps:")
    print(f"  1. Open 'Summary' sheet for sector-wise overview")
    print(f"  2. Share 'All_Clusters' sheet with your mentor (has everything!)")
    print(f"  3. Check 'All_Patterns' for cross-sector duplicates")
    print(f"  4. Review individual sector sheets for detailed analysis")


if __name__ == "__main__":
    main()
